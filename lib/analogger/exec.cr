require "analogger/command_line"
require "analogger/core"

module Analogger
  class Exec
    def self.run
      command_line = CommandLine.new

      analogger = Core.new(command_line)
      analogger.start
    end
  end
end
