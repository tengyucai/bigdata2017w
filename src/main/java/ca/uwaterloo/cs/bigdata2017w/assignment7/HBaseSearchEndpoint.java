package ca.uwaterloo.cs.bigdata2017w.assignment7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Stack;
import java.util.TreeSet;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class HBaseSearchEndpoint extends Configured implements Tool {
  private MapFile.Reader[] index;
  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;
  private int numReducers;

  private Table indexHBase;
  private Table collectionHBase;

  public class HBaseSearchEndpointHandler extends AbstractHandler {

	@Override
	public void handle(String target, Request baseRequest,
	  HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
	  String query = request.getParameter("query");
	  System.out.println("Query: " + query);
      long startTime = System.currentTimeMillis();
      Map<Integer, String> result = runQuery(query);
      System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");
	  JSONArray jsonArray = new JSONArray();
	  for (Integer key : result.keySet()) {
	  	JSONObject jsonObj = new JSONObject();
	  	jsonObj.put("docid", key);
	  	jsonObj.put("text", result.get(key));
	  	jsonArray.add(jsonObj);
      }
	  
	  response.setContentType("application/json");
	  response.setStatus(HttpServletResponse.SC_OK);
	  response.getWriter().write(jsonArray.toString());
	  baseRequest.setHandled(true);
	}
  }

  private HBaseSearchEndpoint() {}

  private void initialize(Table index, Table collection) throws IOException {
    indexHBase = index;
    collectionHBase = collection;
    stack = new Stack<>();
  }

  private Map<Integer, String> runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    Map<Integer, String> result = new HashMap<Integer, String>();  
    for (Integer i : set) {
      String line = fetchLine(i);
      result.put(i, line);
    }

    return result;
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<>();

    for (PairOfInts pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }

  private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
    ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();

    Get get = new Get(Bytes.toBytes(term));
    Result result = indexHBase.get(get);

    Set<Map.Entry<byte[], byte[]>> keyValues = 
    	result.getFamilyMap(BuildInvertedIndexHBase.CF).entrySet();

    for (Map.Entry<byte[], byte[]> keyValue : keyValues) {
    	int docno = Bytes.toInt(keyValue.getKey());
    	int tf = Bytes.toInt(keyValue.getValue());
    	postings.add(new PairOfInts(docno, tf));
    }

    return postings;
  }

  public String fetchLine(long offset) throws IOException {
  	Get get = new Get(ByteBuffer.allocate(Long.BYTES).putLong(offset).array());
  	Result result = collectionHBase.get(get);
  	byte[] bytes = result.getValue(BuildInvertedIndexHBase.CF, InsertCollectionHBase.DOC);

    String d = new String(bytes, "UTF-8");
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  private static final class Args {
  	@Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
    public String config;

    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    public String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    public String collection;

    @Option(name = "-port", metaVar = "[port]", required = true, usage = "port")
    public int port = 8080;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }

    Configuration conf = getConf();
    conf.addResource(new Path(args.config));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    Connection connection = ConnectionFactory.createConnection(hbaseConfig);
    Table index = connection.getTable(TableName.valueOf(args.index));
    Table collection = connection.getTable(TableName.valueOf(args.collection));

    initialize(index, collection);

    Server server = new Server(args.port);
    server.setHandler(new HBaseSearchEndpointHandler());
	server.start();
	server.join();

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new HBaseSearchEndpoint(), args);
  }
}
