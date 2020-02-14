require 'sidekiq'
require 'sidekiq/logger'

module Sidekiq
  class Logger
    module Processors
      private

      def process_message(message)
        case message
        when Exception
          {
            status: 'exception',
            message: message.message
          }
        when Hash
          if message['retry']
            {
              status: 'retry',
              message: "#{message['class']} failed, retrying with args #{message['args']}."
            }
          else
            {
              status: 'dead',
              message: "#{message['class']} failed with args #{message['args']}, not retrying."
            }
          end
        else
          result = message.split(' ')
          status = result[0].match(/^(start|done|fail):?$/) || []

          {
            status: status[1],                                   # start or done
            run_time: status[1] && result[1] && result[1].to_f,  # run time in seconds
            message: message
          }
        end
      end
    end
    module Formatters
      class JSON
        class V1 < Sidekiq::Logger::Formatters::Base
          include Processors

          def call(severity, time, program_name, message)
            {
              '@timestamp' => time.utc.iso8601,
              '@fields' => {
                pid: ::Process.pid,
                tid: "TID-#{Thread.current.object_id.to_s(36)}",
                context: ctx.to_s,
                program_name: program_name,
                worker: ctx.to_s.split(' ')[0]
              },
              '@type' => 'sidekiq',
              '@status' => nil,
              '@severity' => severity,
              '@run_time' => nil
            }.merge(parsed_message(message)).to_json + "\n"
          end

          # Add @ prefix to JSON properties
          def parsed_message(message)
            process_message(message).map { |k, v| ["@#{k}", v] }.to_h
          end
        end

        class V2 < Sidekiq::Logger::Formatters::Base
          include Processors

          def call(severity, time, program_name, message)
            {
              timestamp: time.utc.iso8601,
              fields: {
                pid: ::Process.pid,
                tid: tid.to_s,
                context: ctx.to_s,
                program_name: program_name,
                worker: ctx.to_s.split(' ')[0]
              },
              type: 'sidekiq',
              status: nil,
              severity: severity,
              run_time: nil
            }.merge(process_message(message)).to_json + "\n"
          end
        end
      end
    end
  end
end
