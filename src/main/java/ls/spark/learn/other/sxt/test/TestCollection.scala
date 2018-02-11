package ls.spark.learn.other.sxt.test

object TestCollection {
	def main(args: Array[String]) {

		var t = List(1,2,3,5,5)
//		println("---001--"+t(2))


//		map 个位置相加  函数编程
//		println(t.map(a=> {print("***"+a); a+2}));
		
		
//		println(t.map(_+1));
	  
		var t2 = t.+:("test");//添加元素
//		println(6::t2);
//		println(t2);
//		t2 = t::6::Nil;//组成新的List  t作为一个元素
//		println(t2);

//		t2.foreach(t=>print("---+++"+t))


//		println("/--***---"+t.distinct)
//		println("---+++++********************Slice"+t.slice(0, 2))
		
//		println("-*--*--*--*--*--*--*--*--*-")
//		for(temp<-t2){
//		  print(temp)
//		}

//		println("-*--*--*--*--*--*--*--*--*-")
//		for(i <- 0 to t2.length-1){
//		  println(i)
//		  println(t2(i))
//		}


//		println("-*--*--*--*--*--*--*--*--*-")
//		println(t./:(100)({
//		    (sum,num)=>print(sum+"--"+num+" ");
//			sum-num
//		}));
		
		// 1,2,3,5,5
//		println(t.reduce(_-_))
		

//		println("-*--*--*--*--*--*--*--*--*-")
//		println(t.foldLeft(10)((sum,num)=>{print(sum+"--"+num+" ");
//			num+sum;
//		}));

//		println("-*--*--*--*--*--*--*--*--*-")
//		println(t.map(v =>v+2));

		println("-*--*--*--*--*--*--*--*--*-")
//		元组
		var tuple01 = (1,5,6,6);
		println(tuple01._1)
		println(tuple01._4)
		
	}
}