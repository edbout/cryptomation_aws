# dashboard.rb
require 'sinatra'
require 'sinatra/reloader'
require 'json'

# Config
LOG_PATH = File.join(File.dirname(__FILE__), 'log', 'bot.log')
PORT   = 4567
HOST   = '0.0.0.0'

set :bind, HOST
set :port, PORT

# Turn off default Sinatra logging so it doesn't pollute our log
disable :logging

def read_log_lines(n = 1000)
  if File.exist?(LOG_PATH)
    # Force read into a variable and ensure it is not empty
    lines = File.readlines(LOG_PATH, encoding: "UTF-8")
    puts "DEBUG: read_log_lines – #{lines.size} lines read"
    
    # Return the last N lines, or all if less than N
    result = lines.length > n ? lines[-n..-1] : lines
    result || []
  else
    ["ERROR: log file not found: #{LOG_PATH}"]
  end
end

def styled_log_lines(lines)
  lines.map do |line|
    line = CGI.escapeHTML(line)
    line = line.gsub(/(ERROR|crash|fatal)/i) { |m| "<mark class='err'>#{m}</mark>" }
    line = line.gsub(/(INFO|debug|DEBUG)/i) { |m| "<mark class='info'>#{m}</mark>" }
    line.strip
  end
end

get '/' do
  lines = read_log_lines(2000)
  highlighted = styled_log_lines(lines)

  content_type :html

  erb :index, locals: {
    title: "Polymarket Bot Log",
    log_lines: highlighted,
    log_path: LOG_PATH,
    time: Time.now.strftime("%Y-%m-%d %H:%M:%S")
  }
end

# Auto‑refresh endpoint (hit /health or / for periodic JS reload)
get '/health' do
  { ok: true, time: Time.now.utc.iso8601 }.to_json
end
