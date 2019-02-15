package dgraph

import io.dgraph.DgraphProto.Operation
import io.dgraph.{DgraphClient, DgraphGrpc}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, Metadata}
import io.grpc.stub.MetadataUtils

/**
  * Created by PerkinsZhu on 2019/2/15 11:50
  **/
object DGraphTool {
  private val TEST_HOSTNAME = "120.132.21.196"
  private val TEST_PORT = 9080
  val client = getClient(false)

  def main(args: Array[String]): Unit = {
    val schema = "name: string @index(exact) ."
    testCreateSchema(schema)

  }

  def getClient(withAuthHeader: Boolean): DgraphClient = {
    val channel = ManagedChannelBuilder.forAddress(TEST_HOSTNAME, TEST_PORT).usePlaintext(true).build
    var stub = DgraphGrpc.newStub(channel)
    if (withAuthHeader) {
      val metadata = new Metadata
      metadata.put(Metadata.Key.of("auth-token", Metadata.ASCII_STRING_MARSHALLER), "the-auth-token-value")
      stub = MetadataUtils.attachHeaders(stub, metadata)
    }
    new DgraphClient(stub)
  }

  def testCreateSchema(schema: String): Unit = {
    val op = Operation.newBuilder.setSchema(schema).build
    client.alter(op)
  }

}
