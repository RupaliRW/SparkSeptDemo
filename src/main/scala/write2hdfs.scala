package org.itc.com
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, OutputStreamWriter}

case class Employee(id: Int, name: String, department: String)


object write2hdfs {
  def main(args: Array[String]): Unit = {
    // Create a list of 10 employees
    val employees = List(
      Employee(1, "John Doe", "IT"),
      Employee(2, "Jane Smith", "HR")
    )

    // Specify the HDFS path where you want to write the data
    val hdfsPath = "novbatch_h/rupali/a.csv"

    // Create a Hadoop Configuration
    val conf = new Configuration()
    conf.set("fs.defaultFS", "ip-172-31-3-80.eu-west-2.compute.internal:8020")
    // Get the Hadoop FileSystem object
    val fs = FileSystem.get(conf)

    // Create a new file in HDFS
    val outputPath = new Path(hdfsPath)
    val outputStream = fs.create(outputPath)

    // Write the employee data to the file
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    try {
      employees.foreach { employee =>
        writer.write(s"${employee.id},${employee.name},${employee.department}\n")
      }
    } finally {
      writer.close()
    }

    // Close the FileSystem
    fs.close()
  }
}
