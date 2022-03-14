package QueryEngine

import org.apache.jena.sparql.algebra.op.{OpFilter, _}
import org.apache.jena.sparql.algebra.walker.OpVisitorByType
import org.apache.jena.sparql.algebra.{Op, OpVisitor}

object AlgebraWalker{
  def walkBottomUp(visitor:OpVisitor, op:Op) {
    println(visitor)
    println(op)
    op.visit(new AlgebraWalker(visitor, false))
  }
}

class AlgebraWalker extends OpVisitorByType {
  var visitor:OpVisitor = _
  var topDown:Boolean = false

  def this(visitor:OpVisitor, topDown:Boolean) {
    this()
    this.visitor = visitor
    this.topDown = topDown
  }

  def DUMMY(): Unit ={

  }
  def walkTopDown(visitor:OpVisitor , op:Op) {
    op.visit(new AlgebraWalker(visitor, true))
  }

  def walkBottomUp(visitor:OpVisitor, op:Op) {
    op.visit(new AlgebraWalker(visitor, false))
  }

  def visit0(op:Op0){
    op.visit(visitor)
  }
  def visit1(op:Op1)
  {

  }
  def visit2(op:Op2)
  {

  }
  def visitN(op:OpN)
  {

  }
  override def visitExt(op:OpExt)
  {
    op.visit(visitor)
  }
  def visitFilter(op:OpFilter)
  {
    visit1(op)
  }
}