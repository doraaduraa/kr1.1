require 'csv'
require 'bigdecimal'
require 'time'

class StreamingCsvParser
  def initialize(io, headers: true, col_sep: ',', casters: {})
    @io = io
    @headers = headers
    @col_sep = col_sep
    @casters = normalize_casters(casters)
  end

  def each
    return enum_for(:each) unless block_given?

    csv_opts = { col_sep: @col_sep, headers: @headers }
    CSV.new(@io, **csv_opts).each do |row|
      if @headers
        h = row.to_hash
       
        h.keys.each do |k|
          v = h[k]
          casted = apply_caster(k, v)
          h[k] = casted
          
          if k.respond_to?(:to_sym)
            h[k.to_sym] = casted
          end
        end
        yield h
      else
        a = row.map.with_index { |v, i| apply_caster(i, v) }
        yield a
      end
    end
  end

  private

  def normalize_casters(casters)
    {}.tap do |out|
      casters.each do |k, v|
        if k.is_a?(Integer)
          out[k] = v
        else
          s = k.to_s
          sym = (s.to_sym rescue nil)
          out[s] = v
          out[sym] = v if sym
        end
      end
    end
  end

  def apply_caster(key, val)
    return nil if val.nil? || val == ''
    caster = if @casters.key?(key)
               @casters[key]
             elsif key.respond_to?(:to_sym) && @casters.key?(key.to_sym)
               @casters[key.to_sym]
             elsif key.respond_to?(:to_s) && @casters.key?(key.to_s)
               @casters[key.to_s]
             else
               nil
             end

    return val if caster.nil?

    case caster
    when :int
      Integer(val) rescue nil
    when :decimal
      begin
        BigDecimal(val)
      rescue ArgumentError, TypeError
        nil
      end
    when :time
      begin
        Time.parse(val)
      rescue ArgumentError, TypeError
        nil
      end
    when Proc
      begin
        caster.call(val)
      rescue => e
        raise e
      end
    else
      if caster.respond_to?(:call)
        caster.call(val)
      else
        val
      end
    end
  end
end
