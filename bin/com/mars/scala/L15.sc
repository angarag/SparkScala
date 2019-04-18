
object L15 {
  println("Welcome to the Scala worksheet")
  def parseLine(line: String){
  val fields = line.split(',')
  val age = fields(2).toInt
  val numFriends = fields(3).toInt;
  (age, numFriends)
  }
  val sc = new SparkContext("local[*]", "RatingsCounter")
  val lines = sc.textFile("../fakefriends.csv")
  val rdd = lines.map(parseLine)
  
}