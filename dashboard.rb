require 'sinatra'
require 'sinatra/reloader'
require 'json'
require 'cgi'
require 'redis'
require 'dotenv'
require 'net/http'
require 'uri'
require 'date'

Dotenv.load(File.join(File.dirname(__FILE__), '.env'))

LOG_PATH = File.join(File.dirname(__FILE__), 'log', 'bot.log')
PORT     = 4567
HOST     = '0.0.0.0'

set :bind, HOST
set :port, PORT
disable :logging

helpers do
  def dir_cls(d)
    d == 'UP' ? 'pos' : (d.empty? ? 'dim' : 'neg')
  end
end

# ── Redis ────────────────────────────────────────────────────────────────────
def rdb
  @rdb ||= Redis.new(url: ENV.fetch('REDISCLOUD_URL', 'redis://localhost:6379'))
end

# ── Log helpers ───────────────────────────────────────────────────────────────
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

# ── Polymarket results ────────────────────────────────────────────────────────
POLY_USER      = '0x4fF44F5E2c039122Daab3047F03D390AACda8915'
POLY_ACTIVITY  = 'https://data-api.polymarket.com/activity'
POLY_POSITIONS = 'https://data-api.polymarket.com/positions'
POLY_SYMS      = %w[btc eth sol xrp doge]
RESULTS_START  = Date.new(2026, 5, 1)
RESULTS_TTL    = 300  # seconds before re-fetching

$results_cache    = nil
$results_cache_ts = nil

def poly_fetch(url, extra_params = {}, limit: 500)
  rows   = []
  offset = 0
  loop do
    uri       = URI(url)
    uri.query = URI.encode_www_form({ user: POLY_USER, limit: limit, offset: offset }.merge(extra_params))
    res       = Net::HTTP.get_response(uri)
    break unless res.is_a?(Net::HTTPSuccess)
    batch = JSON.parse(res.body)
    break if batch.empty?
    rows.concat(batch)
    break if batch.size < limit
    offset += limit
    sleep 0.2
  end
  rows
rescue => e
  warn "poly_fetch error #{url}: #{e}"
  []
end

def poly_symbol(slug)
  s = slug.to_s.downcase
  POLY_SYMS.each do |sym|
    if s.include?(sym) && (s.start_with?("#{sym}-") || s.include?("-#{sym}-") || s.end_with?("-#{sym}"))
      return sym.upcase
    end
  end
  'OTHER'
end

def poly_flow(type, side, usdc)
  return  usdc if %w[REDEEM REDEMPTION].include?(type)
  return  usdc if type == 'TRADE' && side == 'SELL'
  return -usdc if type == 'TRADE' && side == 'BUY'
  0.0
end

def load_results
  now = Time.now
  return $results_cache if $results_cache && $results_cache_ts && (now - $results_cache_ts) < RESULTS_TTL

  raw_act = poly_fetch(POLY_ACTIVITY)
  raw_pos = poly_fetch(POLY_POSITIONS)

  # ── Parse activity ──────────────────────────────────────────────────────────
  activity = raw_act.map do |r|
    date = Time.at(r['timestamp'].to_f).utc.to_date
    {
      date:      date,
      weekday:   date.strftime('%A'),
      usdc:      r['usdcSize'].to_f,
      side:      r['side'].to_s.upcase,
      type:      r['type'].to_s.upcase,
      symbol:    poly_symbol(r['eventSlug']),
      cond_id:   r['conditionId'].to_s,
    }
  end

  sym_set = POLY_SYMS.map(&:upcase)

  cashflow = activity.select do |r|
    r[:date] >= RESULTS_START &&
    sym_set.include?(r[:symbol]) &&
    %w[TRADE REDEEM REDEMPTION].include?(r[:type])
  end
  cashflow.each { |r| r[:flow] = poly_flow(r[:type], r[:side], r[:usdc]) }

  # ── Parse positions (active only) ───────────────────────────────────────────
  positions = raw_pos
    .select  { |r| r['currentValue'].to_f > 0 }
    .map     do |r|
      { symbol:    poly_symbol(r['slug']),
        size:      r['size'].to_f,
        avg_price: r['avgPrice'].to_f,
        cur_value: r['currentValue'].to_f,
        cash_pnl:  r['cashPnl'].to_f,
        redeemable: r['redeemable'] == true }
    end
    .select { |r| sym_set.include?(r[:symbol]) }

  # ── Overall by symbol ───────────────────────────────────────────────────────
  overall = sym_set.each_with_object({}) do |sym, h|
    rows = cashflow.select { |r| r[:symbol] == sym }
    next if rows.empty?
    bought   = rows.select { |r| r[:side] == 'BUY' }.sum { |r| r[:usdc] }.round(2)
    sold     = rows.select { |r| r[:side] == 'SELL' }.sum { |r| r[:usdc] }.round(2)
    redeemed = rows.select { |r| %w[REDEEM REDEMPTION].include?(r[:type]) }.sum { |r| r[:usdc] }.round(2)
    net_pnl  = rows.sum { |r| r[:flow] }.round(2)
    h[sym]   = {
      trades:   rows.count { |r| r[:type] == 'TRADE' },
      bought:   bought,
      sold:     sold,
      redeemed: redeemed,
      net_pnl:  net_pnl,
      roi:      bought > 0 ? (net_pnl / bought * 100).round(1) : nil,
    }
  end

  # ── Daily breakdown ─────────────────────────────────────────────────────────
  daily = cashflow
    .group_by { |r| [r[:date].to_s, r[:symbol]] }
    .map do |(date, sym), rows|
      buys    = rows.select { |r| r[:side] == 'BUY' }
      sells   = rows.select { |r| r[:side] == 'SELL' }
      redeems = rows.select { |r| %w[REDEEM REDEMPTION].include?(r[:type]) }
      { date:        date,
        symbol:      sym,
        trades:      rows.count { |r| r[:type] == 'TRADE' },
        redeems:     redeems.size,
        buy_usdc:    buys.sum { |r| r[:usdc] }.round(2),
        sell_usdc:   sells.sum { |r| r[:usdc] }.round(2),
        redeem_usdc: redeems.sum { |r| r[:usdc] }.round(2),
        pnl:         rows.sum { |r| r[:flow] }.round(2) }
    end
    .sort_by { |r| [-r[:date].tr('-', '').to_i, r[:symbol]] }

  # ── Weekly patterns ─────────────────────────────────────────────────────────
  day_order = %w[Monday Tuesday Wednesday Thursday Friday Saturday Sunday]
  weekly = cashflow
    .group_by { |r| [r[:weekday], r[:symbol]] }
    .map do |(day, sym), rows|
      { day:    day,
        symbol: sym,
        count:  rows.size,
        bought: rows.select { |r| r[:side] == 'BUY' }.sum { |r| r[:usdc] }.round(2),
        pnl:    rows.sum { |r| r[:flow] }.round(2) }
    end
    .sort_by { |r| [day_order.index(r[:day]) || 99, r[:symbol]] }

  # ── Positions summary ───────────────────────────────────────────────────────
  pos_by_sym = sym_set.each_with_object({}) do |sym, h|
    rows = positions.select { |r| r[:symbol] == sym }
    next if rows.empty?
    h[sym] = {
      count:      rows.size,
      cur_value:  rows.sum { |r| r[:cur_value] }.round(2),
      unrealized: rows.sum { |r| r[:cash_pnl] }.round(2),
      redeemable: rows.count { |r| r[:redeemable] },
    }
  end

  # ── Totals ──────────────────────────────────────────────────────────────────
  totals = {
    pnl:        cashflow.sum { |r| r[:flow] }.round(2),
    bought:     cashflow.select { |r| r[:side] == 'BUY' }.sum { |r| r[:usdc] }.round(2),
    sold:       cashflow.select { |r| r[:side] == 'SELL' }.sum { |r| r[:usdc] }.round(2),
    redeemed:   cashflow.select { |r| %w[REDEEM REDEMPTION].include?(r[:type]) }.sum { |r| r[:usdc] }.round(2),
    unrealized: positions.sum { |r| r[:cash_pnl] }.round(2),
    open_count: positions.size,
  }
  totals[:total_pnl] = (totals[:pnl] + totals[:unrealized]).round(2)

  $results_cache    = { overall: overall, daily: daily, weekly: weekly,
                        positions: pos_by_sym, totals: totals,
                        fetched_at: now.strftime('%Y-%m-%d %H:%M:%S') }
  $results_cache_ts = now
  $results_cache
end

# ── Trade history from Redis ─────────────────────────────────────────────────
def load_trades
  keys = rdb.keys('order:*').sort
  trades = keys.map do |key|
    all = rdb.hgetall(key)
    next nil unless all['data']
    begin
      d = JSON.parse(all['data'])
    rescue
      next nil
    end

    entry_price = d['price'].to_f
    # Skip old/malformed orders — valid Polymarket token prices are always 0–1
    next nil unless entry_price > 0 && entry_price < 1

    asset      = (all['asset'] || d['asset']).to_s.upcase
    side       = d['side'].to_s.upcase
    raw_time   = (d['created_at'] || d['entry_time']).to_s
    entry_time = raw_time[0, 19].tr('T', ' ')

    # WebSocket consensus (Bybit/Coinbase/Chainlink) — written at bar close
    bar_dir  = all['bar_direction'].to_s.upcase   # UP / DOWN
    bar_pct  = all['bar_pct'].to_f
    bar_cb   = all['bar_coinbase'].to_s.upcase
    bar_cl   = all['bar_chainlink'].to_s.upcase
    bar_con  = all['bar_consensus'].to_s.upcase   # consensus direction
    bar_agree = all['bar_agree'].to_s             # e.g. "2/3"

    # Polymarket final verdict (token resolution)
    pm_out    = all['polymarket_direction'].to_s.upcase
    pm_pct    = all['polymarket_pct'].to_f
    pm_status = all['polymarket_status'].to_s

    # WIN/LOSS: compare side vs Polymarket outcome (final truth); fall back to consensus
    verdict = if !pm_out.empty?
      (side == 'YES') == (pm_out == 'YES') ? 'WIN' : 'LOSS'
    elsif !bar_con.empty?
      ((side == 'YES') == (bar_con == 'UP')) ? 'WIN~' : 'LOSS~'
    end

    {
      order_id:   d['order_id'].to_s,
      asset:      asset,
      side:       side,
      entry_price: entry_price,
      size:       d['size'].to_f.round(2),
      entry_time: entry_time,
      market_slug: (all['market_slug'] || d['market_slug']).to_s,
      bar_dir:    bar_dir,
      bar_pct:    bar_pct,
      bar_cb:     bar_cb,
      bar_cl:     bar_cl,
      bar_con:    bar_con,
      bar_agree:  bar_agree,
      pm_out:     pm_out,
      pm_pct:     pm_pct,
      pm_status:  pm_status,
      verdict:    verdict,
    }
  end.compact
  trades.sort_by { |t| t[:entry_time] }.reverse
end

# ── Routes ────────────────────────────────────────────────────────────────────
get '/' do
  lines       = read_log_lines(1000)
  highlighted = styled_log_lines(lines).reverse
  erb :index, locals: {
    title:     'Polymarket Bot Log',
    log_lines: highlighted,
    log_path:  LOG_PATH,
    time:      Time.now.strftime('%Y-%m-%d %H:%M:%S')
  }
end

get '/stats' do
  keys  = rdb.keys('stats:trade:*').sort
  stats = keys.each_with_object({}) do |key, h|
    asset = key.sub('stats:trade:', '')
    raw   = rdb.hgetall(key)

    align_pass  = raw['alignment_pass'].to_i
    align_fail  = raw['alignment_fail'].to_i
    align_total = align_pass + align_fail
    align_rate  = align_total > 0 ? (align_pass.to_f / align_total * 100).round(1) : nil

    clob_fail  = raw['clob_fail'].to_i
    placed     = raw['order_placed'].to_i
    clob_total = placed + clob_fail
    clob_rate  = clob_total > 0 ? (placed.to_f / clob_total * 100).round(1) : nil

    tp      = raw['tp'].to_i
    sl      = raw['sl'].to_i
    trail   = raw['trail_stop'].to_i
    expiry  = raw['expiry_close'].to_i
    correct = raw['correct_direction'].to_i
    closed  = tp + sl + trail + expiry
    win_rate = closed > 0 ? (correct.to_f / closed * 100).round(1) : nil

    sig_key = "prices:signals:#{asset}"
    fv_key  = "prices:fairvalue:#{asset}"
    sig_total   = rdb.zcard(sig_key).to_i
    sig_pending = rdb.zcount(sig_key, '-inf', '+inf').to_i  # all; pending = ending :na
    # count members ending in :na via zrangebyscore scan (approximate via ZSCAN pattern)
    sig_na = begin
      rdb.zrange(sig_key, 0, -1).count { |m| m.end_with?(':na') }
    rescue
      0
    end
    fv_total = rdb.zcard(fv_key).to_i

    h[asset] = {
      alignment_pass: align_pass, alignment_fail: align_fail, alignment_rate: align_rate,
      clob_fail: clob_fail, order_placed: placed, clob_rate: clob_rate,
      tp: tp, sl: sl, trail_stop: trail, expiry_close: expiry,
      correct: correct, closed: closed, win_rate: win_rate,
      sig_total: sig_total, sig_na: sig_na, fv_total: fv_total
    }
  end
  erb :stats, locals: { stats: stats, time: Time.now.strftime('%Y-%m-%d %H:%M:%S') }
end

get '/results' do
  if params[:refresh]
    $results_cache    = nil
    $results_cache_ts = nil
  end
  data   = load_results
  trades = load_trades
  erb :results, locals: { data: data, start_date: RESULTS_START.to_s, trades: trades }
end

get '/health' do
  { ok: true, time: Time.now.utc.iso8601 }.to_json
end
