package fs;

/** Simple generator class to create new FS object from a config parameter. */
public class FSFactory {
  public static FSReader get(FSReader.FS type) {
    switch (type) {
      case HADOOP: return new HadoopReader();
      case LOCAL: return new LocalReader();
      default:
        return null;
    }
  }

  public static FSReader get(String type) {
    try {
      return get(FSReader.FS.valueOf(type));
    } catch (ClassCastException e) {
      throw new ClassCastException("Failed to parse FSReader type. Are you " +
        "using the FSReader.FS enum? " + e.getLocalizedMessage());
    }
  }
  
  public static FSReader get(Object type) {
    return get(type.toString());
  }
}
