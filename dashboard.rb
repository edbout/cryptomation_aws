# dashboard.rb
require 'sinatra'
require 'sinatra/reloader' if development?
require 'json'

# Config
LOG_PATH = File.join(File.dirname(__FILE__), 'log', 'bot.log')
PORT   = 4567
HOST   = '0.0.0.0'

set :bind, HOST
set :port, PORT

# Turn off default Sinatra logging so it doesn't pollute our log
disable :logging

# Helper to read last N lines of log
def read_log_lines(n = 1000)
  if File.exist?(LOG_PATH)
    lines = File.readlines(LOG_PATH)
    lines[-n..-1] || []
  else
    ["ERROR: log file not found: #{LOG_PATH}"]
  end
end

# Optional: filter / highlight certain lines
def styled_log_lines(lines)
  lines.map do |line|
    line = CGI.escapeHTML(line)

    # Highlight errors / important markers
    line = line.gsub(/(ERROR|TP|SL|crash|fatal)/i) { |m| "<mark class='err'>#{m}</mark>" }
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


__END__

@@layout
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title><%= title %></title>
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: monospace;
      font-size: 14px;
      background: #000;
      color: #eee;
      padding: 10px;
    }
    header {
      background: #222;
      padding: 10px;
      margin-bottom: 10px;
      border-radius: 4px;
      font-size: 16px;
      font-weight: bold;
    }
    header small {
      display: block;
      font-size: 12px;
      opacity: 0.7;
    }
    pre {
      margin: 0;
      padding: 8px;
      background: #111;
      border-radius: 4px;
      border: 1px solid #333;
      white-space: pre-wrap;
      word-wrap: break-word;
      max-height: calc(100vh - 120px);
      overflow-y: auto;
    }
    mark.info {
      background: #003300;
      color: #6ddb6d;
      padding: 0 2px;
    }
    mark.err {
      background: #400;
      color: #f00;
      font-weight: bold;
      padding: 0 2px;
    }
    footer {
      margin-top: 10px;
      font-size: 12px;
      opacity: 0.7;
    }
  </style>
</head>
<body>
  <header><%= title %>
    <small>Path: <%= log_path %> | Time: <%= time %></small>
  </header>

  <main>
    <pre><%= log_lines.join("\n") %></pre>
  </main>

  <footer>Auto‑refreshing every 5 seconds. Press F5 to refresh manually.</footer>

  <script>
    // Simple auto‑refresh logic
    setInterval(() => {
      fetch('/health')
        .then(r => r.json())
        .then(json => {
          document.querySelector('small').textContent =
            `Path: <%= log_path %> | Time: ${json.time}`;
        })
        .catch(() => {});
    }, 5000);
  </script>
</body>
</html>