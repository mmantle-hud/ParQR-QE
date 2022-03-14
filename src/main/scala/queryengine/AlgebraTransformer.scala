package queryengine

import org.apache.jena.shared.PrefixMapping
import org.apache.jena.sparql.algebra.OpVisitorBase
import org.apache.jena.sparql.algebra.op.{OpReduced, OpSlice, _}

import scala.collection.JavaConverters._

class AlgebraTransformer extends OpVisitorBase{

  var prefixes:PrefixMapping = _


  override def visit(opBGP:OpBGP) {

    val list = opBGP.getPattern().getList().asScala
    list.foreach(x=>{
      x.getMatchSubject().isVariable

      val subjStr=if(x.getMatchSubject().isVariable){
        x.getSubject().toString
      }else{
        val fullSubjStr = x.getSubject().toString
        "<"+fullSubjStr+">"
      }

      val objStr=if(x.getMatchObject().isVariable){
        x.getObject().toString
      }else{
        val fullObjStr = x.getObject().toString
        "<"+fullObjStr+">"
      }

      val pred = if(x.getPredicate().toString().indexOf("#") > -1){
        x.getPredicate().toString.split("#")(1)
      }else{
        val predStr = x.getPredicate().toString
        predStr.substring(predStr.lastIndexOf("/")+1)
      }

      Triples.instance().addTriple((
        subjStr,
        pred,
        objStr))
    })
  }
  override def visit(opBGP:OpFilter ) {
    val expr = opBGP.getExprs().getList().asScala
    val fnc = expr(0).getFunction()
    val rel = fnc.getFunctionIRI()
    val args =fnc.getArgs().asScala
    val theVar = args(0).getExprVar()
    val theConst = args(1).getConstant()
    val relStr = rel.toString
    Triples.instance().addTriple((theVar.toString,relStr.substring(relStr.lastIndexOf("/")+1),theConst.toString))
    opBGP.getSubOp().visit(new AlgebraTransformer())
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
  override def visit(opBGP:OpLeftJoin  ) {
    println("Left Join")
    println(opBGP)
  }
  override def visit(opBGP:OpUnion  ) {
    println("Union")
    println(opBGP)
  }
  override def visit(opBGP:OpProject   ) {
    opBGP.getSubOp().visit(new AlgebraTransformer())
  }
  override def visit(opBGP:OpDistinct  ) {
    println("distinct")
    println(opBGP)
  }
  override def visit(opBGP:OpOrder   ) {
    println("order")
    println(opBGP)
  }
  override def visit(opBGP:OpSlice   ) {
    println("slice")
    println(opBGP)
  }
  override def visit(opBGP:OpReduced   ) {
    println("reduced")
    println(opBGP)

  }
}
