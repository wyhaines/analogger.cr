require "analogger/core/daemonize"
require "analogger/core/log"
require "analogger/core/exec_arguments"
require "analogger/timer"
require "analogger/destination_registry"
require "analogger/destination/file"

struct Number
  def positive?
    self > 0
  end
end

module Analogger
  class Core
    DEFAULT_SEVERITY_LEVELS = [
      "debug",
      "info",
      "warn",
      "error",
      "fatal",
    ].each_with_object({} of String => Bool) { |k, h| h[k] = true }

    property config : Config
    property invocation_arguments : ExecArguments
    class_property default_log : String = "STDOUT"
    class_property default_log_destination : Analogger::Destination::File | IO::FileDescriptor | Nil

    class NoPortProvided < RuntimeError
      def initialize
        @message = "The port to bind to was not provided."
      end
    end

    class BadPort < RuntimeError
      def initialize(port : String | Int32 | Nil)
        @message = "The port provided (#{port}) is invalid."
      end
    end

    EXIT_SIGNALS    = [Signal::INT, Signal::TERM]
    RELOAD_SIGNALS  = [Signal::HUP]
    RESTART_SIGNALS = [Signal::USR2]

    def initialize(command_line : CommandLine)
      @config = command_line.config
      @invocation_arguments = ExecArguments.new(command: File.expand_path(PROGRAM_NAME), args: ARGV)
      @logs = Hash(String, Log).new { |h, k| h[k] = Log.new(service: k) }
      @queue = Hash(String, Array(String)).new { |h, k| h[k] = [] of String }
      @rcount = 0
      @wcount = 0
      @now = Time.local
    end

    def start
      handle_daemonize
      setup_signal_traps
      postprocess_config_load
      check_config_settings
      populate_logs
      set_config_defaults
      create_periodic_timers

      puts "Start me with #{config.inspect}"
      puts "LOGS: #{@logs.inspect}"
    end

    def setup_signal_traps
      safe_trap(signal_list: EXIT_SIGNALS) { handle_pending_and_exit }
      safe_trap(signal_list: RELOAD_SIGNALS) { cleanup_and_reopen }
      safe_trap(signal_list: RESTART_SIGNALS) do
        Process.exec(
          command: invocation_arguments.command,
          args: invocation_arguments.args
        )
      end
    end

    def safe_trap(signal_list : Array(Signal), &operation)
      signal_list.each do |sig|
        sig.trap { operation.call }
      end
    end

    def handle_daemonize
      daemonize if @config.daemonize
      File.open(@config.pidfile.to_s, "w+") { |fh| fh.puts Process.pid } if @config.pidfile
    end

    def normalize_levels(levels : String)
      levels.split(/,/).each_with_object({} of String => Bool) { |k, h| h[k.to_s] = true }
    end

    def normalize_levels(levels : Array)
      levels.each_with_object({} of String => Bool) { |k, h| h[k.to_s] = true }
    end

    def normalize_levels(levels : Bool | Nil)
      DEFAULT_SEVERITY_LEVELS
    end

    def normalize_levels(levels : Hash)
      levels
    end

    def normalize_levels(levels : Int32 | YAML::Any)
      {levels.to_s => true}
    end

    def postprocess_config_load
      @config.logs ||= [] of Analogger::Config::Log
      @config.levels = normalize_levels(@config.levels) if @config.levels

      @config.logs.each do |log|
        log.levels = normalize_levels(log.levels)
      end
    end

    def check_config_settings
      raise NoPortProvided.new unless @config.port
      raise BadPort.new(@config.port) unless @config.port.to_s.to_i.positive?
    end

    # Iterate through the logs entries in the configuration file, and create a log entity for each one.
    def populate_logs
      @config.logs.each do |log|
        next unless log.service

        service_array = log.service.as?(Array)
        if service_array
          service_array.each do |loglog|
            new_log = Log.new(service: loglog,
              levels: normalize_levels(log.levels),
              raw_destination: log.logfile,
              destination: logfile_destination(log.logfile, log.type, log.options),
              cull: log.cull,
              type: log.type,
              options: log.options)
            @logs[new_log.service] = new_log
          end
        else
          service_string = log.service.as?(String) || nil
          new_log = Log.new(service: service_string,
            levels: normalize_levels(log.levels),
            raw_destination: log.logfile,
            destination: logfile_destination(log.logfile, log.type, log.options),
            cull: log.cull,
            type: log.type,
            options: log.options)
          @logs[new_log.service] = new_log
        end
      end
    end

    def logfile_destination(logfile : IO, type : String | Nil = "file", options : Array(String) | Nil = ["ab"])
      type ||= "file"
      type = type.downcase
      return logfile if logfile == STDERR || logfile == STDOUT
      logfile.reopen(logfile) if logfile.respond_to? :reopen
    end

    def logfile_destination(logfile : String, type : String | Nil = "file", options : Array(String) | Nil = ["ab"])
      type ||= "file"
      type = type.to_s.downcase

      if logfile =~ /^STDOUT$/i
        STDOUT
      elsif logfile =~ /^STDERR$/i
        STDERR
      else
        obj = Analogger::DestinationRegistry.get(type)
        obj.open(logfile, options)
      end
    end

    def set_config_defaults
      @config.host ||= "127.0.0.1"

      @config.interval = @config.interval.nil? ? 1 : @config.interval.to_i
      @config.syncinterval = @config.syncinterval.nil? ? 60 : @config.syncinterval.to_i
      Analogger::Core.default_log = @config.default_log.to_s.blank? ? "STDOUT" : @config.default_log.to_s
      Analogger::Core.default_log_destination = logfile_destination(logfile: Analogger::Core.default_log)
      @logs["default"] = Log.new
    end

    def create_periodic_timers
      @clock_update_timer = Analogger::Timer.new(periodic: true) { @now = Time.local }
      @write_queue_timer = Analogger::Timer.new(
        interval: @config.interval.to_i,
        periodic: true
        ) { write_queue }
      @flush_queue_timer = Analogger::Timer.new(
        interval: @config.syncinterval.to_i,
        periodic: true
        ) { flush_queue }
    end

    def write_queue
    end

    def flush_queue
    end

    def handle_pending_and_exit
    end

    def cleanup
      @logs.each do |_service, l|
        if !(dest = l.destination).nil?
          if dest.closed? && dest.responds_to?(:fsync)
            dest.fsync
          end
          dest.close unless dest.closed? || [STDERR, STDOUT].includes?(dest)
        end
      end
    end

    def cleanup_and_reopen
      @logs.each do |_service, l|
        if !(dest = l.destination).nil?
          if dest.closed? && dest.responds_to?(:fsync)
            dest.fsync
          end
          if dest.responds_to?(:reopen)
            dest.reopen(dest) if ![STDERR, STDOUT].includes?(dest)
          end
        end
      end
    end
  end
end
