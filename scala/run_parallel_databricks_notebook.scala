import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, Future, Await, blocking}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

def runNotebookParallel(notebookPath: String, numberofThreads: Int, parametersList: List[Map[String, String]], maxRetries: Int): List[String] ={
	val notebookContext = dbutils.notebook.getContext()
	val threadPool = Executors.newFixedThreadPool(numberofThreads)
	implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)
	
	def nunNotebook(params: Map[String, String]): Future[String] = Future {
		dbutils.notebook.setContext(notebookContext)
		Try {
			dbutils.notebook.run(notebookPath, 60, params)
		} match {
			case Success(result) => result
			case Failure(ex) => println(s"An error occurred: ${ex.getMessage}")
			"Error"
		}
	}
	
	def runWithRetry(params: Map[String, String], retries: Int = maxRetries): Future[String] = {
		nunNotebook(params).recoverWith {
			case _ if retries > 0 => println(s"Retrying... attempts remaining: $retries")
				runWithRetry(params, retries -1)
		}
	}
	
	val futures: List[Future[String]] = parametersList.map {
		params => runWithRetry(params)
	}
	
	val allResult: Future[List[String]] = Future.sequence(futures)
	allResult.onComplete {
		case Success(results) => println("All tasks completed successfully")
//		results.foreach(println)
		case Failure(ex) => println(s"One or more task failed with error: ${ex.getMessage}")
	}
	
	val result: List[String] = try {
		blocking {
			Await.result(allResult, Duration.Inf)
		}
	} finally {
		threadPool.shutdown()
		if (! threadPool.awaitTermination(1, TimeUnit.MINUTES)) {
			threadPool.shutdownNow()
		}
	}
	return result
}


// Sample Usage
val params: Map[String, String] = Map("p1" -> "v1", "p2" -> "v2")
val notebookPath: String = "./notebookName"
val numberOfThreads: Int = 2
val parametesList: List[Map[String, String]] = List(params, params, params)
val maxRetries: Int = 3

val output: List[String] = runNotebookParallel(notebookPath, numberOfThreads, parametesList, maxRetries)