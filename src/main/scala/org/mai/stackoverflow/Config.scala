package org.mai.stackoverflow

import org.mai.stackoverflow.Commands._

case class Config(
                   command: Command = null,

                   // load
                   path: String = null,
                   append: Boolean = false,
                   // clean
                   dropTables: Boolean = false,
                   // init
                   force: Boolean = false,
                   // extract
                   query: String = null,
                   file: String = null
                 ) {
  override def toString: String = {
    command match {
      case Load => s"$Load(path=`$path`, append=$append)"
      case Clean => s"$Clean(dropTables=$dropTables)"
      case Init => s"$Init(force=$force)"
      case Extract => s"$Extract(query=`$query`, file=`$file`)"
      case _ => "NoCommand()"
    }
  }
}
