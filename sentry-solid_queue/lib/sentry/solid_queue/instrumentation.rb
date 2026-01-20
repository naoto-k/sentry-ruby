# frozen_string_literal: true

require "sentry/solid_queue/context_filter"

module Sentry
  module SolidQueue
    module Instrumentation
      def self.included(base)
        base.around_enqueue do |job, block|
          next block.call unless Sentry.initialized?
          next block.call unless job.class.queue_adapter_name == "solid_queue"

          # around_enqueue (Client-side) logic
          sentry_context = {
            "user" => Sentry.get_current_scope.user,
            "trace_propagation_headers" => Sentry.get_trace_propagation_headers
          }

          # Compact context to avoid sending empty keys
          sentry_context.compact!

          job.arguments << { "_sentry_context" => sentry_context } unless sentry_context.empty?

          Sentry.with_child_span(op: "queue.publish", description: job.class.name) do |span|
            if span
              span.set_data(Span::DataConventions::MESSAGING_MESSAGE_ID, job.job_id)
              span.set_data(Span::DataConventions::MESSAGING_DESTINATION_NAME, job.queue_name)
            end

            block.call
          end
        end

        base.around_perform do |job, block|
          next block.call unless Sentry.initialized?
          next block.call unless job.class.queue_adapter_name == "solid_queue"

          # Extract and remove Sentry context from arguments
          sentry_context = if job.arguments.last.is_a?(Hash) && job.arguments.last.key?("_sentry_context")
            job.arguments.pop["_sentry_context"]
          end || {}

          # Setup Scope
          Sentry.clone_hub_to_current_thread
          scope = Sentry.get_current_scope

          # Restore user from context
          if (user = sentry_context["user"])
            scope.set_user(user)
          end

          context = {
            "class" => job.class.name,
            "job_id" => job.job_id,
            "queue_name" => job.queue_name,
            "arguments" => job.arguments # Arguments are now clean
          }

          scope.set_contexts(solid_queue: context)
          scope.set_tags(queue: job.queue_name, jid: job.job_id)

          context_filter = Sentry::SolidQueue::ContextFilter.new(context)
          scope.set_transaction_name(context_filter.transaction_name, source: :task)

          # Fix: Assign the started transaction (new or continued)
          transaction = Sentry.start_transaction(transaction: Sentry.continue_trace(
            sentry_context["trace_propagation_headers"] || {},
            name: scope.transaction_name,
            op: "queue.process",
            origin: "auto.queue.solid_queue"
          ))
          if transaction
            scope.set_span(transaction)

            transaction.set_data(Span::DataConventions::MESSAGING_MESSAGE_ID, job.job_id)
            transaction.set_data(Span::DataConventions::MESSAGING_DESTINATION_NAME, job.queue_name)

            if job.enqueued_at
              latency = ((Time.now - job.enqueued_at) * 1000).to_i
              transaction.set_data(Span::DataConventions::MESSAGING_MESSAGE_RECEIVE_LATENCY, latency)
            end

            retry_count = job.executions
            if retry_count > 0
              transaction.set_data(Span::DataConventions::MESSAGING_MESSAGE_RETRY_COUNT, retry_count)
            end
          end

          begin
            block.call
            transaction&.set_http_status(200)
          rescue Exception => e
            Sentry::SolidQueue::ErrorHandler.new.call(e, context)
            transaction&.set_http_status(500)
            raise
          ensure
            transaction&.finish
          end

          scope.clear
        end
      end
    end
  end
end
