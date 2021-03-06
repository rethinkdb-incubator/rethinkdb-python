from rethinkdb.utilities import chain_to_bytes, EnhancedTuple


def test_chain_to_bytes():
  """
  Test the utility function chain_to_bytes
  """
  input = "test"
  res = chain_to_bytes(input)
  assert res == input.encode("latin-1")
  
  res2 = chain_to_bytes(input, input)
  assert res2 == input.encode("latin-1") + input.encode("latin-1")
  
# Is it worth to test it? It has no public method
# def test_EnhancedTuple():
  
