/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss.guard;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.cloud.core.metadata.DirListingMetadata;
import org.apache.hadoop.cloud.core.metadata.MetadataStore;
import org.apache.hadoop.cloud.core.metadata.MetadataUtils;
import org.apache.hadoop.cloud.core.metadata.NullMetadataStore;
import org.apache.hadoop.cloud.core.metadata.PathMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_FAIL;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_FOUND;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_SUCCESS;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_USAGE;

/**
 * CLI to manage OSSGuard Metadata Store.
 */
public abstract class OSSGuardTool extends Configured implements Tool {
  private static final Logger LOG =
      LoggerFactory.getLogger(OSSGuardTool.class);

  private static final String NAME = "ossguard";
  private static final String COMMON_USAGE =
      "When possible and not overridden by more specific options, metadata\n" +
      "repository information will be inferred from the OSS URL(if provided)" +
      "\n\n" +
      "Generic options supported are:\n" +
      "  -conf <config file> - specify an application configuration file\n" +
      "  -D <property=value> - define a value for a given property\n";
  private static final String USAGE = NAME +
      " [command] [OPTIONS] [oss://BUCKET]\n\n" +
      "Commands: \n" +
      "\t" + Init.NAME + " - " + Init.PURPOSE + "\n" +
      "\t" + Destroy.NAME + " - " + Destroy.PURPOSE + "\n" +
      "\t" + Import.NAME + " - " + Import.PURPOSE + "\n" +
      "\t" + Diff.NAME + " - " + Diff.PURPOSE + "\n" +
      "\t" + Prune.NAME + " - " + Prune.PURPOSE + "\n" +
      "\t" + SetCapacity.NAME + " - " + SetCapacity.PURPOSE + "\n";

  final CommandFormat commandFormat;

  static final String READ_FLAG = "read";
  static final String WRITE_FLAG = "write";
  private static final String DATA_IN_OSS_IS_PRESERVED
      = "(all data in OSS is preserved)";

  private static final String AGE_OPTIONS_USAGE = "[-days <days>] "
      + "[-hours <hours>] [-minutes <minutes>] [-seconds <seconds>]";

  MetadataStore ms;
  AliyunOSSFileSystem oss;

  // Exit codes
  static final int SUCCESS = EXIT_SUCCESS;
  static final int INVALID_ARGUMENT = EXIT_COMMAND_ARGUMENT_ERROR;
  static final int E_USAGE = EXIT_USAGE;
  static final int ERROR = EXIT_FAIL;

  static final PathFilter PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return true;
    }
  };

  public abstract String getUsage();

  /**
   * Return sub-command name.
   * @return sub-command name.
   */
  public abstract String getName();

  protected OSSGuardTool(Configuration conf, String...opts) {
    super(conf);
    commandFormat = new CommandFormat(0, Integer.MAX_VALUE, opts);
  }

  @Override
  public final int run(String[] args) throws Exception {
    return run(args, System.out);
  }

  /**
   * Run the tool, capturing the output (if the tool supports that).
   *
   * As well as returning an exit code, the implementations can choose to
   * throw an instance of {@link ExitUtil.ExitException} with their exit
   * code set to the desired exit value. The exit code of such an exception
   * is used for the tool's exit code, and the stack trace only logged at
   * debug.
   * @param args argument list
   * @param out output stream
   * @return the exit code to return.
   * @throws Exception on any failure
   */
  public abstract int run(String[] args, PrintStream out) throws Exception,
      ExitUtil.ExitException;


  MetadataStore initMetadataStore(boolean autoCreate) throws IOException {
    if (ms != null) {
      return ms;
    }

    Configuration conf = oss == null ? getConf() : oss.getConf();
    if (!autoCreate) {
      conf.setBoolean(Constants.OTS_TABLE_AUTO_CREATE_KEY, false);
    }

    if (oss == null) {
      ms = OSSGuard.getMetadataStore(conf);
    } else {
      ms = OSSGuard.getMetadataStore(oss);
    }
    return ms;
  }

  /**
   * Create and initialize a new OSS FileSystem instance.
   * @param path OSS URI
   * @param nullStore  create OSS FS with {@link NullMetadataStore} or not
   * @throws IOException failure to init filesystem
   * @throws ExitUtil.ExitException if the FS is not an OSS FS
   */
  protected void initOSSFileSystem(String path, boolean nullStore)
      throws IOException {
    URI uri = toUri(path);
    Configuration conf = getConf();
    if (nullStore) {
      conf.set(Constants.OSS_METADATA_STORE_IMPL,
          NullMetadataStore.class.getName());
    }
    FileSystem fs = FileSystem.newInstance(uri, conf);
    if (!(fs instanceof AliyunOSSFileSystem)) {
      throw new ExitUtil.ExitException(INVALID_ARGUMENT,
          String.format("URI %s is not a OSS file system: %s",
              uri, fs.getClass().getName()));
    }

    oss = (AliyunOSSFileSystem)fs;
  }

  protected void checkIfOSSBucketIsGuarded(List<String> paths, String usage)
      throws IOException {
    if (paths.isEmpty()) {
      System.err.println(usage);
      throw new IOException("OSS path is missing");
    }
    String ossPath = paths.get(0);
    try (AliyunOSSFileSystem fs = (AliyunOSSFileSystem)
        AliyunOSSFileSystem.newInstance(toUri(ossPath), getConf())) {
      Preconditions.checkState(fs.hasMetadataStore(),
          "The OSS bucket is unguarded. " + getName()
              + " can not be used on an unguarded bucket.");
    }
  }

  /**
   * Create the metadata store.
   */
  static class Init extends OSSGuardTool {
    static final String NAME = "init";
    static final String PURPOSE = "initialize metadata repository";
    static final String USAGE = NAME + " [OPTIONS]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "Aliyun OTS specific options:\n" +
        "  -" + READ_FLAG + " UNIT - Read capacity units\n" +
        "  -" + WRITE_FLAG + " UNIT - Write capacity units\n" +
        "\n" +
        "Will read OTS configurations from hadoop configuration\n";

    Init(Configuration conf) {
      super(conf);
      // read capacity.
      commandFormat.addOptionWithValue(READ_FLAG);
      // write capacity.
      commandFormat.addOptionWithValue(WRITE_FLAG);
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public int run(String[] args, PrintStream out)
        throws Exception, ExitUtil.ExitException {
      List<String> paths = parseArgs(commandFormat, args, USAGE);
      checkIfOSSBucketIsGuarded(paths, USAGE);

      String readCap = commandFormat.getOptValue(READ_FLAG);
      if (readCap != null && !readCap.isEmpty()) {
        getConf().setInt(Constants.OTS_TABLE_CAPACITY_READ_KEY,
            Integer.parseInt(readCap));
      }

      String writeCap = commandFormat.getOptValue(WRITE_FLAG);
      if (writeCap != null && !writeCap.isEmpty()) {
        getConf().setInt(Constants.OTS_TABLE_CAPACITY_WRITE_KEY,
            Integer.parseInt(writeCap));
      }

      initMetadataStore(true);
      return SUCCESS;
    }
  }

  /**
   * Destroy a metadata store.
   */
  static class Destroy extends OSSGuardTool {
    static final String NAME = "destroy";
    static final String PURPOSE = "destroy Metadata Store data " +
        DATA_IN_OSS_IS_PRESERVED;

    static final String USAGE = NAME + " [OPTIONS] [oss://BUCKET]\n" +
        "\t" + PURPOSE + "\n\n";

    Destroy(Configuration conf) {
      super(conf);
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public int run(String[] args, PrintStream out)
        throws Exception, ExitUtil.ExitException {
      List<String> paths = parseArgs(commandFormat, args, USAGE);
      checkIfOSSBucketIsGuarded(paths, USAGE);

      try {
        initMetadataStore(false);
      } catch (IOException e) {
        // indication that the table was not found
        System.out.println("Metadata Store does not exist.");
        LOG.debug("Failed to bind to store to be destroyed", e);
        return SUCCESS;
      }

      Preconditions.checkState(ms != null,
          "Metadata Store is not initialized");
      ms.destroy();
      return SUCCESS;
    }
  }

  static class SetCapacity extends OSSGuardTool {
    static final String NAME = "set-capacity";
    static final String PURPOSE = "alter metadata store capacity unit";
    static final String READ_CAP_INVALID = "Read capacity unit must "
        + "have value greater than or equal to 1.";
    static final String WRITE_CAP_INVALID = "Write capacity must "
        + "have value greater than or equal to 1.";
    static final String USAGE = NAME + " [OPTIONS] [oss://BUCKET]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "(implementation-specific)\n" +
        "\n" +
        "Aliyun OTS-specific options:\n" +
        "  -" + READ_FLAG + " UNIT - read throughput capacity units\n" +
        "  -" + WRITE_FLAG + " UNIT - write throughput capacity units\n";

    SetCapacity(Configuration conf) {
      super(conf);
      // read capacity.
      commandFormat.addOptionWithValue(READ_FLAG);
      // write capacity.
      commandFormat.addOptionWithValue(WRITE_FLAG);
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public int run(String[] args, PrintStream out)
        throws Exception, ExitUtil.ExitException {
      List<String> paths = parseArgs(commandFormat, args, USAGE);
      checkIfOSSBucketIsGuarded(paths, USAGE);

      Map<String, String> options = new HashMap<>();
      initMetadataStore(false);

      String readCap = commandFormat.getOptValue(READ_FLAG);
      if (StringUtils.isNotEmpty(readCap)) {
        Preconditions.checkArgument(Integer.parseInt(readCap) > 0,
            READ_CAP_INVALID);

        out.println(String.format("Read capacity set to %s", readCap));
        options.put(Constants.OTS_TABLE_CAPACITY_READ_KEY, readCap);
      }

      String writeCap = commandFormat.getOptValue(WRITE_FLAG);
      if (StringUtils.isNotEmpty(writeCap)) {
        Preconditions.checkArgument(Integer.parseInt(writeCap) > 0,
            WRITE_CAP_INVALID);

        out.println(String.format("Write capacity set to %s", writeCap));
        options.put(Constants.OTS_TABLE_CAPACITY_WRITE_KEY, writeCap);
      }

      ms.updateParameters(options);
      printStoreDiagnostics(out, ms);
      return SUCCESS;
    }
  }

  static class Import extends OSSGuardTool {
    static final String NAME = "import";
    static final String PURPOSE = "import metadata from existing OSS " +
        "data";
    static final String USAGE = NAME + " [OPTIONS] [oss://BUCKET]\n" +
        "\t" + PURPOSE + "\n";

    Import(Configuration conf) {
      super(conf);
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public int run(String[] args, PrintStream out)
        throws Exception, ExitUtil.ExitException {
      List<String> paths = parseArgs(commandFormat, args, USAGE);
      initOSSFileSystem(paths.get(0), false);
      // import OSS file by listLocatedStatus recursive
      String filePath = oss.getUri().getPath();
      if (filePath.isEmpty()) {
        filePath = "/";
      }

      Path path = new Path(filePath);
      long files = 0;
      long dirs = 0;
      RemoteIterator<LocatedFileStatus> itr = oss.listLocatedStatus(
          path, PATH_FILTER, true);
      while (itr.hasNext()) {
        LocatedFileStatus status = itr.next();
        if (status.isDirectory()) {
          dirs++;
        } else {
          files++;
        }
      }

      out.println(
          String.format("Imported %d dirs, %d files into Metadata Store",
              dirs, files));
      return SUCCESS;
    }
  }

  static class Diff extends OSSGuardTool {
    static final String NAME = "diff";
    static final String PURPOSE = "report on delta between OSS and " +
        "repository";
    static final String USAGE = NAME + " [OPTIONS] [oss://BUCKET]\n" +
        "\t" + PURPOSE + "\n";

    static final String SEP = "\t";
    static final String OSS_PREFIX = "OSS";
    static final String MS_PREFIX = "MS";
    Diff(Configuration conf) {
      super(conf);
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    @Override
    public String getName() {
      return NAME;
    }

    private static String format(FileStatus status) {
      return String.format("%s%s%d%s%s",
          status.isDirectory() ? "D" : "F",
          SEP,
          status.getLen(),
          SEP,
          status.getPath().toString());
    }

    private static boolean differ(FileStatus ths, FileStatus rhs) {
      Preconditions.checkArgument(!(ths == null && rhs == null));
      return (ths == null || rhs == null) ||
          (ths.isDirectory() != rhs.isDirectory()) ||
          (ths.getLen() != rhs.getLen()) ||
          (!ths.isDirectory() &&
              ths.getModificationTime() != rhs.getModificationTime());
    }

    private static void printDiff(FileStatus msStatus,
        FileStatus ossStatus,
        PrintStream out) {
      Preconditions.checkArgument(!(msStatus == null && ossStatus == null));
      if (msStatus != null && ossStatus != null) {
        Preconditions.checkArgument(
            msStatus.getPath().equals(ossStatus.getPath()),
            String.format("The path from metadata store and OSS are different:"
                + " ms=%s OSS=%s", msStatus.getPath(), ossStatus.getPath()));
      }

      if (differ(msStatus, ossStatus)) {
        if (ossStatus != null) {
          out.println(
              String.format("%s%s%s", OSS_PREFIX, SEP, format(ossStatus)));
        }
        if (msStatus != null) {
          out.println(
              String.format("%s%s%s", MS_PREFIX, SEP, format(msStatus)));
        }
      }
    }

    private void compareDir(FileStatus msDir, FileStatus ossDir,
        PrintStream out) throws IOException {
      Preconditions.checkArgument(!(msDir == null && ossDir == null),
          "The path does not exist in metadata store and on OSS.");

      if (msDir != null && ossDir != null) {
        Preconditions.checkArgument(msDir.getPath().equals(ossDir.getPath()),
            String.format("The path from metadata store and OSS are different:" +
                    " ms=%s OSS=%s", msDir.getPath(), ossDir.getPath()));
      }

      Map<Path, FileStatus> ossChildren = new HashMap<>();
      if (ossDir != null && ossDir.isDirectory()) {
        for (FileStatus status : oss.listStatus(ossDir.getPath())) {
          ossChildren.put(status.getPath(), status);
        }
      }

      Map<Path, FileStatus> msChildren = new HashMap<>();
      if (msDir != null && msDir.isDirectory()) {
        DirListingMetadata dirMeta = ms.listChildren(msDir.getPath());

        if (dirMeta != null) {
          for (PathMetadata meta : dirMeta.getListing()) {
            FileStatus status = meta.getFileStatus();
            msChildren.put(status.getPath(), status);
          }
        }
      }

      Set<Path> allPaths = new HashSet<>(ossChildren.keySet());
      allPaths.addAll(msChildren.keySet());

      for (Path path : allPaths) {
        FileStatus ossStatus = ossChildren.get(path);
        FileStatus msStatus = msChildren.get(path);
        printDiff(msStatus, ossStatus, out);
        if ((ossStatus != null && ossStatus.isDirectory()) ||
            (msStatus != null && msStatus.isDirectory())) {
          compareDir(msStatus, ossStatus, out);
        }
      }

      out.flush();
    }

    @Override
    public int run(String[] args, PrintStream out)
        throws Exception, ExitUtil.ExitException {
      List<String> paths = parseArgs(commandFormat, args, USAGE);
      initMetadataStore(false);
      initOSSFileSystem(paths.get(0), true);

      URI uri = toUri(paths.get(0));
      Path root;
      if (uri.getPath().isEmpty()) {
        root = new Path("/");
      } else {
        root = new Path(uri.getPath());
      }

      root = oss.makeQualified(root);

      FileStatus ossStatus = null;
      try {
        ossStatus = oss.getFileStatus(root);
      } catch (FileNotFoundException e) {
        // ignored
      }

      PathMetadata meta = ms.get(root);
      FileStatus msStatus = meta == null ? null : meta.getFileStatus();
      compareDir(msStatus, ossStatus, out);
      out.flush();
      return SUCCESS;
    }
  }

  static class Prune extends OSSGuardTool {
    static final String NAME = "prune";
    static final String PURPOSE = "truncate older metadata from " +
        "repository " + DATA_IN_OSS_IS_PRESERVED;
    static final String USAGE = NAME + " [OPTIONS] [oss://BUCKET]\n" +
        "\t" + PURPOSE + "\n\n" +
        "Common options:\n" +
        "(implementation-specific)\n" +
        "Age options. Any combination of these integer-valued options:\n" +
        AGE_OPTIONS_USAGE + "\n";

    // These are common to prune, upload sub commands.
    static final String DAYS_FLAG = "days";
    static final String HOURS_FLAG = "hours";
    static final String MINUTES_FLAG = "minutes";
    static final String SECONDS_FLAG = "seconds";

    Prune(Configuration conf) {
      super(conf);
      commandFormat.addOptionWithValue(DAYS_FLAG);
      commandFormat.addOptionWithValue(HOURS_FLAG);
      commandFormat.addOptionWithValue(MINUTES_FLAG);
      commandFormat.addOptionWithValue(SECONDS_FLAG);
    }

    @Override
    public String getUsage() {
      return USAGE;
    }

    @Override
    public String getName() {
      return NAME;
    }

    private long getDeltaComponent(TimeUnit unit, String arg) {
      String raw = commandFormat.getOptValue(arg);
      if (StringUtils.isEmpty(raw)) {
        return 0;
      }
      Long parsed = Long.parseLong(raw);
      return unit.toMillis(parsed);
    }

    private long ageOptionsToMsec() {
      long delta = 0;
      delta += getDeltaComponent(TimeUnit.DAYS, DAYS_FLAG);
      delta += getDeltaComponent(TimeUnit.HOURS, HOURS_FLAG);
      delta += getDeltaComponent(TimeUnit.MINUTES, MINUTES_FLAG);
      delta += getDeltaComponent(TimeUnit.SECONDS, SECONDS_FLAG);

      return delta;
    }

    @Override
    public int run(String[] args, PrintStream out)
        throws Exception, ExitUtil.ExitException {
      List<String> paths = parseArgs(commandFormat, args, USAGE);
      initMetadataStore(false);

      long delta = ageOptionsToMsec();
      if (delta <= 0) {
        throw new ExitUtil.ExitException(INVALID_ARGUMENT,
            "You must specify a positive age for metadata to prune.");
      }

      long now = System.currentTimeMillis();
      long divide = now - delta;

      // remove the protocol from path string to get keyPrefix
      String prefix = MetadataUtils.pathToParentKey(new Path(paths.get(0)));
      ms.prune(divide, prefix);
      out.flush();
      return SUCCESS;
    }
  }

  private static OSSGuardTool command;

  @VisibleForTesting
  void setStore(MetadataStore ms) {
    this.ms = ms;
  }

  @VisibleForTesting
  void setFileSystem(AliyunOSSFileSystem oss) {
    this.oss = oss;
  }

  private static List<String> parseArgs(CommandFormat commandFormat,
      String[] args, String usage) throws ExitUtil.ExitException {
    try {
      List<String> paths = commandFormat.parse(args, 1);
      if (paths.isEmpty()) {
        System.err.println(usage);
        throw new ExitUtil.ExitException(INVALID_ARGUMENT,
            "OSS path is missing");
      }
      return paths;
    } catch (Exception e) {
      throw new ExitUtil.ExitException(INVALID_ARGUMENT, e.getMessage());
    }
  }

  private static void printHelp() {
    if (command == null) {
      System.err.println("Usage: hadoop " + USAGE);
      System.err.println("\tperform OSSGuard metadata store " +
          "administrative commands.");
    } else {
      System.err.println("Usage: hadoop " + command.getUsage());
    }
    System.err.println();
    System.err.println(COMMON_USAGE);
  }

  /**
   * Retrieve and Print store diagnostics.
   * @param out output stream
   * @param store store
   * @throws IOException Failure to retrieve the data.
   */
  protected static void printStoreDiagnostics(
      PrintStream out, MetadataStore store) throws IOException {
    Map<String, String> diagnostics = store.getDiagnostics();
    out.println("Metadata Store Diagnostics:");
    for (Map.Entry<String, String> entry : diagnostics.entrySet()) {
      out.println(String.format("\t%s=%s", entry.getKey(), entry.getValue()));
    }
  }

  /**
   * Convert a path to a URI, catching any {@code URISyntaxException}
   * and converting to an invalid args exception.
   * @param ossPath path to convert to a URI
   * @return a URI of the path
   * @throws ExitUtil.ExitException INVALID_ARGUMENT if the URI is invalid
   */
  protected static URI toUri(String ossPath) {
    URI uri;
    try {
      uri = new URI(ossPath);
    } catch (URISyntaxException e) {
      throw new ExitUtil.ExitException(INVALID_ARGUMENT,
          String.format("Not a valid filesystem path: %s", ossPath));
    }
    return uri;
  }

  public static int run(Configuration conf, String...args)
      throws Exception {
    /* ToolRunner.run does this too, but we must do it before looking at
    subCommand or instantiating the cmd object below */
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();

    if (otherArgs.length == 0) {
      printHelp();
      throw new ExitUtil.ExitException(E_USAGE, "No arguments provided");
    }

    final String subCommand = otherArgs[0];
    LOG.debug("Executing command {}", subCommand);
    switch (subCommand) {
      case Init.NAME:
        command = new Init(conf);
        break;
      case Destroy.NAME:
        command = new Destroy(conf);
        break;
      case SetCapacity.NAME:
        command = new SetCapacity(conf);
        break;
      case Import.NAME:
        command = new Import(conf);
        break;
      case Diff.NAME:
        command = new Diff(conf);
        break;
      case Prune.NAME:
        command = new Prune(conf);
        break;
      default:
        printHelp();
        throw new ExitUtil.ExitException(E_USAGE,
            "Unknown command " + subCommand);
    }
    return ToolRunner.run(conf, command, otherArgs);
  }

  /**
   * Main entry point. Calls {@code System.exit()} on all execution paths.
   * @param args argument list
   */
  public static void main(String[] args) {
    try {
      int ret = run(new Configuration(), args);
      ExitUtil.terminate(ret, "");
    } catch (CommandFormat.UnknownOptionException e) {
      System.err.println(e.getMessage());
      printHelp();
      ExitUtil.terminate(E_USAGE, e.getMessage());
    } catch (ExitUtil.ExitException e) {
      // explicitly raised exit code
      System.err.println(e.getMessage());
      ExitUtil.terminate(e.status, e.toString());
    } catch (FileNotFoundException e) {
      // Bucket doesn't exist or similar - return code of 44, "404".
      System.err.println(e.toString());
      LOG.debug("Not found:", e);
      ExitUtil.terminate(EXIT_NOT_FOUND, e.toString());
    } catch (Throwable e) {
      e.printStackTrace(System.err);
      ExitUtil.terminate(ERROR, e.toString());
    }
  }
}
