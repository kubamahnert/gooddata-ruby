# Copyright (c) 2010-2018 GoodData Corporation. All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

module GoodData
  module Bricks
    # Logger decorator with ability to mask sensitive values
    class MaskLoggerDecorator
      # entry-point
      # @param [Logger] logger logger to decorated
      # @param [Array] values_to_mask sensitive values to be masked out from logs
      def initialize(logger, values_to_mask = [])
        @logger = logger
        @values_to_mask = values_to_mask
      end

      # log methods to be decorated
      %i[debug error fatal info unknown warn].each do |level|
        define_method level do |message|
          mask message
          @logger.send(level, message)
        end
      end

      def add(severity, message = nil, progname = nil)
        mask message
        mask severity
        super(severity, message, progname)
      end

      private

      # Masks given String.
      # @param message  [String] message to mask
      # @return masked_message [String] masked message
      def mask_string(message)
        unless message.nil?
          @values_to_mask.reduce(message) do |masked_message, value_to_mask|
            masked_message.gsub!(value_to_mask, "******")
            masked_message
          end
        end
      end

      # Masks given message
      # @param message [String] or [Hash] or [Array] message to mask
      # @return masked_message [String] or [Hash] or [Array] masked message
      def mask(message)
        unless message.nil?
          if message.is_a?(String)
            mask_string message
          else
            (message.is_a?(Hash) ? message.values : message).each do |item|
              mask item
            end
          end
        end
      end
    end
  end
end