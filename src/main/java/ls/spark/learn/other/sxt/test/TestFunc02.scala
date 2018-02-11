package ls.spark.learn.other.sxt.test

object TestFunc02 {
	
  def  test() : Unit = {
//	  for(i <- 1.to(100)){
//		  println(i)
//	  }
	  for(i <- 1 to 100 ){
		  println(i)
	  }
	}
	
	def  test2() = {
			for(i <- 1 until 100 ){
				println(i)
			} 
	}
	
	def  test3() = {
		for(i <- 0 to 100 if (i % 2) == 1 ; if (i % 5) > 3 ){
		  println("I: "+i)
		}
	}
	  
//	switch
  def testmatch(n:Int)={
    n match {
    	case 1 => {println("111") ;n;}
//    	break;
    	case 2 => println("2222") ;n;
    	case _ => println("other"); "test";//default
    }
  }
	
  

	def main(args: Array[String]) {
//		test();
//		test2();
//		test3();
		println(testmatch(2));
	  
	
	}
}