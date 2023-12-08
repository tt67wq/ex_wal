defmodule UvarintTest do
  @moduledoc false
  use ExUnit.Case

  alias ExWal.Uvarint

  describe "encode/1" do
    test "encode" do
      assert(Uvarint.encode(0) == <<0::size(8)>>, "encode(0) == <<0::size(8)>>")
      assert(Uvarint.encode(1) == <<1::size(8)>>, "encode(1) == <<1::size(8)>>")
      assert(Uvarint.encode(127) == <<127::size(8)>>, "encode(127) == <<127::size(8)>>")

      assert(
        Uvarint.encode(128) == <<128::size(8), 1::size(8)>>,
        "encode(128) == <<128::size(8), 1::size(8)>>"
      )

      assert(
        Uvarint.encode(270) == <<142::size(8), 2::size(8)>>,
        "encode(270) == <<142::size(8), 2::size(8)>>"
      )
    end
  end

  describe "decode/1" do
    test "decodes number less than 128" do
      assert Uvarint.decode(<<42::size(8)>>) == {42, 1, ""}
    end

    test "decodes number greater than or equal to 128" do
      assert Uvarint.decode(<<0x8E, 0x02>>) == {270, 2, ""}
      assert Uvarint.decode(<<0x8E, 0x02>> <> "Hello World") == {270, 2, "Hello World"}
    end
  end

  describe "mix" do
    test "encode and decode" do
      strings = ["apple", "banana", "cherry", "date"]

      data =
        strings
        |> Enum.reduce("", fn x, acc ->
          y = Uvarint.encode(byte_size(x)) <> x
          acc <> y
        end)

      {size, _, rest} = Uvarint.decode(data)
      assert size == 5
      <<h::bytes-size(size), rest::binary>> = rest
      assert h == "apple"

      {size, _, rest} = Uvarint.decode(rest)
      assert size == 6
      <<h::bytes-size(size), rest::binary>> = rest
      assert h == "banana"

      {size, _, rest} = Uvarint.decode(rest)
      assert size == 6
      <<h::bytes-size(size), rest::binary>> = rest
      assert h == "cherry"

      {size, _, rest} = Uvarint.decode(rest)
      assert size == 4
      <<h::bytes-size(size), rest::binary>> = rest
      assert h == "date"

      assert rest == ""
    end
  end
end
