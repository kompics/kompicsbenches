package se.kth.benchmarks.visualisation.generator

import java.nio.file.{Path, Paths}
import java.io.File
import org.rogach.scallop._
import com.typesafe.scalalogging.StrictLogging
import scala.reflect.io.Directory
import java.io.FileOutputStream
import java.io.InputStream
import java.io.PrintWriter

object Main extends StrictLogging {
  def main(args: Array[String]): Unit = {
    val conf = new Conf(args);
    logger.debug(s"severing=${conf.serve()} to ${conf.target()} (force? ${conf.force()})");

    val target = prepareFolder(conf);
    copyJs(target);
    textToFile(StandardStyle.styleSheetText, "standard.css", target);
    val index = textToFile(IndexPage.generate(), "index.html", target);
    logger.info("**** All done! ****");
    if (conf.open()) {
      Runtime.getRuntime().exec(s"open ${index.getAbsolutePath()}");
    }
  }

  private def prepareFolder(conf: Conf): File = {
    val folder = if (conf.target.isSupplied) {
      logger.debug(s"User supplied folder ${conf.target()}");
      conf.target().toFile
    } else {
      val normalised = conf.target().toAbsolutePath().normalize();
      val parent = normalised.getParent();
      if ((parent != null) && (parent.endsWith("generator"))) {
        // shift this one up, so it generates the deployment folder at the project root
        val grandparent = parent.getParent();
        assert(grandparent != null);
        val relativePath = parent.relativize(normalised);
        val newPath = grandparent.resolve(relativePath);
        logger.debug(s"Shifted target to project root: $newPath");
        newPath.toFile()
      } else {
        logger.debug(s"Default target has no parent: $normalised");
        conf.target().toFile
      };
    };
    if (folder.exists()) {
      if (folder.isDirectory()) {
        if (folder.canRead() && folder.canWrite()) {
          val contents = folder.list();
          logger.debug(s"Target contents (size=${contents.size}): ${contents.mkString("\n  - ", "\n  - ", "\n")}");
          if (contents.isEmpty) {
            logger.debug("Folder checks out."); // yay
          } else if (conf.force()) {
            val dir = new Directory(folder);
            if (dir.deleteRecursively()) {
              logger.debug("Cleaned out folder."); // yay
              folder.mkdirs();
            } else {
              logger.error(s"Failed to clean out $folder. Unable to proceed.");
              System.exit(1);
            }
          } else {
            logger.error(s"Folder $folder is not empty. Specify '--force' to override anyway. Unable to proceed.");
            System.exit(1);
          }
        } else {
          logger.error(s"Folder $folder has insufficient rights. Unable to proceed.");
          System.exit(1);
        }
      } else {
        logger.error(s"Target $folder is not a folder. Unable to proceed.");
        System.exit(1);
      }
    } else {
      folder.mkdirs();
    }
    logger.info(s"Prepared folder $folder");
    folder
  }

  private def textToFile(text: String, name: String, target: File): File = {
    val file = target.toPath().resolve(name).toFile();
    assert(file.createNewFile(), s"Could not create file $name");
    val output = new PrintWriter(file);
    try {
      output.write(text);
    } finally {
      output.close();
    }
    logger.info(s"Wrote $file");
    file
  }

  private def copyJs(target: File): Unit = {
    import java.nio.charset.StandardCharsets;

    logger.debug("Trying for assembled JS");
    var mapStream: InputStream = null;
    var inputStream = this
      .getClass()
      .getClassLoader()
      .getResourceAsStream(
        "public/benchmark-suite-plotting-opt.js"
      );
    if (inputStream == null) {
      logger.debug("Trying for run JS");
      inputStream = this
        .getClass()
        .getClassLoader()
        .getResourceAsStream(
          "public/benchmark-suite-plotting-fastopt.js"
        );
      mapStream = this
        .getClass()
        .getClassLoader()
        .getResourceAsStream(
          "public/benchmark-suite-plotting-fastopt.js.map"
        );
    }
    if (inputStream == null) {
      logger.error("JS not found. Unable to proceed.");
      System.exit(1);
    }
    // val data = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    // println(s"Got data");
    val jsFile = target.toPath().resolve("benchmark-suite-plotting.js").toFile();
    assert(jsFile.createNewFile(), "Could not create JS file");
    val output = new FileOutputStream(jsFile);
    try {
      val res = inputStream.transferTo(output);
      assert(res > 0L, "Copy did not succeed");
    } finally {
      inputStream.close();
      output.close();
    }
    logger.info(s"Copied JS Data to $jsFile");
    if (mapStream != null) {
      val mapFile = target.toPath().resolve("benchmark-suite-plotting-fastopt.js.map").toFile();
      assert(mapFile.createNewFile(), "Could not create JS Map file");
      val output = new FileOutputStream(mapFile);
      try {
        val res = mapStream.transferTo(output);
        assert(res > 0L, "Copy did not succeed");
      } finally {
        mapStream.close();
        output.close();
      }
      logger.info(s"Copied JS Map to $mapFile");
    }
  }
}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val serve = toggle(default = Some(false), descrYes = "Serve the generated site locally.");
  val target = opt[Path](default = Some(Paths.get(".", "deploy")),
                         descr = "The folder to generate content into. Will be created, if it does not exist.");
  val force = toggle(
    default = Some(false),
    descrYes = "Override non-empty target folders. WARNING: This will delete everything in the target folder!",
    descrNo = "Fail if the target folder is not empty."
  );
  val open = toggle(default = Some(false), descrYes = "Open the generated index file after completion.");
  verify()
}
