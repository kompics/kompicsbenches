package se.kth.benchmarks.visualisation.generator

import java.nio.file.{Path, Paths}
import java.io.File
import org.rogach.scallop._
import com.typesafe.scalalogging.StrictLogging
import scala.reflect.io.Directory
import java.io.FileOutputStream
import java.io.InputStream
import java.io.PrintWriter
import scala.util.{Failure, Success, Try}
import java.io.FileFilter

object Main extends StrictLogging {
  def main(args: Array[String]): Unit = {
    val conf = new Conf(args);
    logger.debug(s"Generating from ${conf.source()} to ${conf.target()} (force? ${conf.force()})");

    val target = prepareFolder(conf);
    val index_res = for {
      _js <- resourceToFile("public/benchmark-suite-plotting-opt.js", "benchmark-suite-plotting.js", target)
        .recoverWith {
          case ex =>
            logger.warn("Failed assembled JS", ex);
            val res: Try[File] =
              resourceToFile("public/benchmark-suite-plotting-fastopt.js", "benchmark-suite-plotting.js", target)
                .flatMap(js => {
                  logger.info("Succeeded with run JS. Also copying mapping file...");
                  resourceToFile("public/benchmark-suite-plotting-fastopt.js.map",
                                 "benchmark-suite-plotting-fastopt.js.map",
                                 target).map(_ => js)
                });
            res
        };
      _main <- resourceToFile("public/main.css", "main.css", target);
      _bootstrap <- resourceToFile("public/bootstrap.min.css", "bootstrap.min.css", target);
      _standard <- textToFile(StandardStyle.styleSheetText, "standard.css", target);
      plots <- generatePlots(conf.source(), target);
      index <- textToFile(IndexPage(plots).generate(), "index.html", target)
    } yield index;
    index_res match {
      case Success(index) => {
        logger.info("**** All done! ****");
        if (conf.open()) {
          Runtime.getRuntime().exec(s"open ${index.getAbsolutePath()}");
        }
      }
      case Failure(ex) => {
        logger.error("A failure occurred during generation", ex);
        System.exit(1);
      }
    }
  }

  private def generatePlots(source: Path, target: File): Try[List[Plot]] = {
    val targetPath = target.toPath();
    val sources: Try[List[File]] = Try {
      val normalised = source.toAbsolutePath().normalize().resolve("summary").toFile();
      assert(normalised.exists(), s"Path $normalised does not exist!");
      assert(normalised.canRead(), s"Can't read in ${normalised}!");
      normalised
        .listFiles(new FileFilter {
          override def accept(file: File): Boolean = {
            file.isFile() && file.getName().endsWith(".data")
          }
        })
        .toList
    };
    sources.flatMap(files => {
      val empty: Try[List[Plot]] = Try(List.empty[Plot]);
      files.foldLeft(empty) { (accTry: Try[List[Plot]], sourceFile: File) =>
        accTry.flatMap(acc => {
          for {
            plotted <- Plotter.fromSource(sourceFile);
            written <- textToFile(plotted.text, plotted.fileName, target)
          } yield Plot(plotted.title, plotted.fileName) :: acc
        })
      }
    })
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

  private def textToFile(text: String, name: String, target: File): Try[File] = {
    val file = target.toPath().resolve(name).toFile();
    assert(file.createNewFile(), s"Could not create file $name");
    val output = new PrintWriter(file);
    val res = Try {
      output.write(text);
      logger.info(s"Wrote $file");
      file
    };
    output.close();
    res
  }

  private def resourceToFile(resource: String, name: String, target: File): Try[File] = {
    var inputStream = this
      .getClass()
      .getClassLoader()
      .getResourceAsStream(resource);
    val file = target.toPath().resolve(name).toFile();
    var output: FileOutputStream = null;
    val res = Try {
      assert(file.createNewFile(), s"Could not create file $file");
      output = new FileOutputStream(file);
      val res = inputStream.transferTo(output);
      inputStream.close();
      inputStream = null;
      output.close();
      output = null;
      assert(res > 0L, "Copy did not succeed");
      logger.info(s"Wrote $file");
      file
    }.recoverWith {
      case _: NullPointerException => {
        if (inputStream != null) {
          inputStream.close();
        }
        if (output != null) {
          output.close();
        }
        file.delete(); // cleanup
        Failure(new RuntimeException(s"Invalid resource path: $resource"))
      }
      case ex => {
        if (inputStream != null) {
          inputStream.close();
        }
        if (output != null) {
          output.close();
        }
        file.delete(); // cleanup
        Failure(ex)
      }
    };
    res
  }

}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  //val serve = toggle(default = Some(false), descrYes = "Serve the generated site locally.");
  val source = opt[Path](descr = "The directory with Experiment Results to generate interactive plots for.")
  val target = opt[Path](default = Some(Paths.get(".", "deploy")),
                         descr = "The folder to generate content into. Will be created, if it does not exist.");
  val force = toggle(
    default = Some(false),
    descrYes = "Override non-empty target folders. WARNING: This will delete everything in the target folder!",
    descrNo = "Fail if the target folder is not empty."
  );
  val open = toggle(default = Some(false), descrYes = "Open the generated index file after completion.");

  requireOne(source);
  verify()
}
