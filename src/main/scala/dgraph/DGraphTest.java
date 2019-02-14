package dgraph;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import io.grpc.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by PerkinsZhu on 2019/2/14 16:54
 **/
public class DGraphTest {

    private DgraphClient client = null;

    public static void main(String[] args) {
        DGraphTest test = new DGraphTest();
        test.initClient();
        test.createSchema();
//        test.testLongMutation();
    }

    private void createSchema() {
        String schema = "test-name: string @index(exact) .";
        DgraphProto.Operation op = DgraphProto.Operation.newBuilder().setSchema(schema).build();
        client.alter(op);
        //        client.alter(DgraphProto.Operation.newBuilder().setDropAll(true).build());

    }

    private void initClient() {
      /*  ManagedChannel channel = ManagedChannelBuilder.forAddress("120.132.21.196", 8080).usePlaintext().build();
        DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);
        // client = new DgraphClient(Collections.singletonList(stub));
        client = new DgraphClient(stub);*/

        ManagedChannel channel = ManagedChannelBuilder.forAddress("120.132.21.196", 9080).usePlaintext(true).build();
        DgraphGrpc.DgraphStub stub = DgraphGrpc.newStub(channel);
        ClientInterceptor timeoutInterceptor = new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                return next.newCall(method, callOptions.withDeadlineAfter(5000, TimeUnit.MILLISECONDS));
            }
        };
        stub.withInterceptors(timeoutInterceptor);
        client = new DgraphClient(stub);
    }

    public void testLongMutation() {
        StringBuilder sb = new StringBuilder();
        sb.append("<_:bob> <name> \" testName\" .");
        try (Transaction txn = client.newTransaction()) {
            DgraphProto.Mutation mutation = DgraphProto.Mutation.newBuilder().setSetNquads(ByteString.copyFromUtf8(sb.toString())).build();
            DgraphProto.Assigned ag = txn.mutate(mutation);
            System.out.println("Got new id:" + ag.getUidsOrThrow("bob"));
            txn.commit();
        }
    }

}
