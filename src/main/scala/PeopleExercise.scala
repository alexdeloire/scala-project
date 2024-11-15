package fr.umontpellier.ig5

import scala.io.Source

object PeopleExercise {

  case class User(id: Int, name: String, age: Int, city: String)

  def main(args: Array[String]): Unit = {
    // Load the data from CSV file
    val source = Source.fromFile("data/people.csv")
    val lines = source.getLines().toList

    // Skip the header row and parse the remaining lines
    val users = lines.tail.map { line =>
      val Array(id, name, age, city) = line.split(",").map(_.trim)
      User(id.toInt, name, age.toInt, city)
    }
    source.close()

    // Filter users aged 25 and above
    val usersOver25 = users.filter(_.age >= 25)

    println("Users aged 25 and above:")
    usersOver25.foreach(println)

    // Extract names and cities
    val extractedData = users.map(user => (user.name, user.city))

    println("\nExtracted names and cities:")
    extractedData.foreach(println)

    // Group users by city
    val groupedByCity = users.groupBy(_.city)

    println("\nGrouped users by city:")
    groupedByCity.foreach { case (city, userList) =>
      println(s"$city -> ${userList.mkString(", ")}")
    }
  }
}

