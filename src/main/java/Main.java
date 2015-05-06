public class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) throw new IllegalArgumentException("Please input file directory and schema " +
      "path as storm -jar Main FILE_PATH SCHEMA_PATH");
    
    Parser parser = new Parser(args[0], args[1]);
    parser.run();
  }
}