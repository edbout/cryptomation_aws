require 'sinatra'
require 'sinatra/reloader'
require 'json'
require 'cgi'
require 'redis'
require 'dotenv'

Dotenv.load(File.join(File.dirname(__FILE__), '.env'))

LOG_PATH = File.join(File.dirname(__FILE__), 'log', 'bot.log')
PORT     = 4567
HOST     = '0.0.0.0'

set :bind, HOST
set :port, PORT
disable :logging

def rdb
  @rdb ||= Redis.new(url: ENV.fetch('REDISCLOUD_URL', 'redis://localhost:6379'))
end

def read_log_lines(n = 1000)
  if File.exist?(LOG_PATH)
    lines = File.readlines(LOG_PATH, encoding: 'UTF-8')
    lines.length > n ? lines[-n..-1] : lines
  else
    ["ERROR: log file not found: #{LOG_PATH}"]
  end
end

def styled_log_lines(lines)
  lines.map do |line|
    line = CGI.escapeHTML(line)
    line = line.gsub(/(ERROR|crash|fatal)/i) { |m| "<mark class='err'>#{m}</mark>" }
    line = line.gsub(/(WARNING|WARN)/i)      { |m| "<mark class='warn'>#{m}</mark>" }
    line = line.gsub(/(INFO|debug|DEBUG)/i)  { |m| "<mark class='info'>#{m}</mark>" }
    line.strip
  end
end

get '/' do
  lines       = read_log_lines(1000)
  highlighted = styled_log_lines(lines)
  erb :index, locals: {
    title:     'Polymarket Bot Log',
    log_lines: highlighted,
    log_path:  LOG_PATH,
    time:      Time.now.strftime('%Y-%m-%d %H:%M:%S')
  }
end

get '/stats' do
  keys = rdb.keys('stats:trade:*').sort
  stats = keys.each_with_object({}) do |key, h|
    asset = key.sub('stats:trade:', '')
    raw   = rdb.hgetall(key)

    align_pass  = raw['alignment_pass'].to_i
    align_fail  = raw['alignment_fail'].to_i
    align_total = align_pass + align_fail
    align_rate  = align_total > 0 ? (align_pass.to_f / align_total * 100).round(1) : nil

    clob_fail   = raw['clob_fail'].to_i
    placed      = raw['order_placed'].to_i
    clob_total  = placed + clob_fail
    clob_rate   = clob_total > 0 ? (placed.to_f / clob_total * 100).round(1) : nil

    tp      = raw['tp'].to_i
    sl      = raw['sl'].to_i
    trail   = raw['trail_stop'].to_i
    expiry  = raw['expiry_close'].to_i
    correct = raw['correct_direction'].to_i
    closed  = tp + sl + trail + expiry
    win_rate = closed > 0 ? (correct.to_f / closed * 100).round(1) : nil

    h[asset] = {
      alignment_pass: align_pass,
      alignment_fail: align_fail,
      alignment_rate: align_rate,
      clob_fail:      clob_fail,
      order_placed:   placed,
      clob_rate:      clob_rate,
      tp:             tp,
      sl:             sl,
      trail_stop:     trail,
      expiry_close:   expiry,
      correct:        correct,
      closed:         closed,
      win_rate:       win_rate
    }
  end

  erb :stats, locals: { stats: stats, time: Time.now.strftime('%Y-%m-%d %H:%M:%S') }
end

get '/health' do
  { ok: true, time: Time.now.utc.iso8601 }.to_json
end
