require_relative 'test_helper'
require_relative '../lib/streaming_csv_parser'
require 'stringio'

class TestStreamingCsvParser < Minitest::Test
  def setup
    @csv = <<~CSV
      id,name,price,created_at
      1,Apple,1.23,2020-01-01 12:00:00
      2,Banana,0.5,2020-02-02 13:30:00
      3,Orange,,
    CSV

    @io = StringIO.new(@csv)
  end

  def test_parses_and_casts
    casters = { id: :int, price: :decimal, created_at: :time }
    parser = StreamingCsvParser.new(@io, headers: true, casters: casters)

    rows = parser.each.to_a
    assert_equal 3, rows.size
    assert_equal 1, rows[0][:id]
    assert_instance_of BigDecimal, rows[0][:price]
    assert_instance_of Time, rows[0][:created_at]

    assert_nil rows[2][:price]
    assert_nil rows[2][:created_at]
  end

  def test_custom_proc_caster
    io = StringIO.new("a,b\n10,foo\n")
    casters = { 'a' => proc { |v| v.to_i * 2 } }
    parser = StreamingCsvParser.new(io, headers: true, casters: casters)
    row = parser.each.first
    assert_equal 20, row['a']
  end

  def test_no_headers_returns_array
    io = StringIO.new("1,2,3\n4,5,6\n")
    casters = { 0 => :int, 2 => :int }
    parser = StreamingCsvParser.new(io, headers: false, casters: casters)
  rows = parser.each.to_a
    assert_equal 1, rows[0][0]
    assert_equal 3, rows[0][2]
  end

  def test_invalid_decimal_returns_nil
    io = StringIO.new("x\nnot_a_decimal\n")
    parser = StreamingCsvParser.new(io, headers: true, casters: { 'x' => :decimal })
    row = parser.each.first
    assert_nil row['x']
  end
end
