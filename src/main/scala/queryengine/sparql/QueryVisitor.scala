package queryengine.sparql

import org.apache.jena.sparql.algebra.{OpVisitor, OpVisitorBase}
import org.apache.jena.sparql.algebra.op._

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.asScalaBufferConverter

class QueryVisitor extends OpVisitorBase() {
  private var triples = ArrayBuffer[(String,String,String)]()


  def getTriples():ArrayBuffer[(String,String,String)]={
    this.triples
  }

  override def visit(opBGP:OpBGP) {
   // println("BGP")
//    println(opBGP)
    val list = opBGP.getPattern().getList().asScala
    list.foreach(x=>{
      x.getMatchSubject().isVariable
//       println("Subject:"+x.getSubject()+":"+x.getMatchSubject().isVariable)
//       println("Predicate:"+x.getPredicate()+":"+x.getMatchPredicate().isVariable)
//       println("Object:"+x.getObject()+":"+x.getMatchObject().isVariable)

      val subjStr = if(x.getMatchSubject().isVariable){
        x.getSubject().toString
      }else{
        val fullSubjStr = x.getSubject().toString
        //        "<"+fullSubjStr.substring(fullSubjStr.lastIndexOf("/")+1)+">"
        "<"+fullSubjStr+">"
      }

      val objStr = if(x.getMatchObject().isVariable){
        x.getObject().toString
      }else{
        val fullObjStr = x.getObject().toString
        "<"+fullObjStr+">"
        //"<"+fullObjStr.substring(fullObjStr.lastIndexOf("/")+1)+">"
      }

      val pred = if(x.getPredicate().toString().indexOf("#") > -1){
        x.getPredicate().toString.split("#")(1)
      }else{
        val predStr = x.getPredicate().toString
        predStr.substring(predStr.lastIndexOf("/")+1)
      }

      this.triples += ((subjStr, pred, objStr))

    })
  }

  override def visit(opBGP:OpFilter ) {
    val expr = opBGP.getExprs().getList().asScala
    val fnc = expr(0).getFunction()
    val rel = fnc.getFunctionIRI()
    val args =fnc.getArgs().asScala
    val theVar = args(0).getExprVar()
    val theConst = args(1).getConstant()
//    println("Filter")
//    println("Function:"+rel)
//    println("Using variable:"+theVar)
//    println("Using the constant:"+theConst)


    this.triples += ((theVar.toString, rel.substring(rel.lastIndexOf("/")+1), theConst.toString))
    opBGP.getSubOp().visit(this)
  }
  override def visit(opBGP:OpJoin ) {
    println("Join")
    println(opBGP)
  }
  override def visit(opBGP:OpPath ) {
    println("Path")
    println(opBGP)
  }
  override def visit(opBGP:OpSequence  ) {
    println("Sequence")
    println(opBGP)
  }
  override def visit(opBGP:OpLeftJoin) {
    println("Left Join")
    println(opBGP)
  }
  override def visit(opBGP:OpUnion) {
    println("Union")
    println(opBGP)
  }
  override def visit(opBGP:OpProject) {
    //println("project")
    //println(opBGP)
    opBGP.getSubOp().visit(this)
  }
  override def visit(opBGP:OpDistinct) {
    println("distinct")
    println(opBGP)
  }
  override def visit(opBGP:OpOrder) {
    println("order")
    println(opBGP)
  }
  override def visit(opBGP:OpSlice) {
    println("slice")
    println(opBGP)
  }
  override def visit(opBGP:OpReduced) {
    println("reduced")
    println(opBGP)

  }

}
