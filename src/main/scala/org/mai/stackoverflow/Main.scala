package org.mai.stackoverflow

import com.opencsv.CSVWriter
import org.mai.stackoverflow.Commands.{Clean, Extract, Init, Load}
import scalikejdbc.config.DBs
import scalikejdbc.{NamedDB, _}

import java.nio.file.{Files, Paths}
import java.sql.SQLException

object Main extends App {

  ConfigParser.parse(args, Config()) match {
    case Some(config) =>
      config.command match {
        case Load => load(config.path, config.append)
        case Clean => clean(config.dropTables)
        case Init => init(config.force)
        case Extract => extract(config.query, config.file)
      }
    case _ =>
  }

  def load(path: String, append: Boolean): Unit = {
    val loader = new DataLoader {
      override def basePath: String = path
    }

    try {
      val entities = loader.loadData()
      val (users, posts, comments) = Logic.splitEntities(entities)

      val db = 'so
      DBs.setup(db)
      implicit val session: DBSession = NamedDB(db).autoCommitSession()

      if (!append) {
        clearAllTables
      }
      insertUsers(users)
      insertPosts(posts)
      insertComments(comments)
    } catch {
      case e: Exception => println(s"Error on load. Message: ${e.getMessage}")
    }
  }

  def clean(dropTables: Boolean): Unit = {
    try {
      val db = 'so
      DBs.setup(db)
      implicit val session: DBSession = NamedDB(db).autoCommitSession()

      if (dropTables) {
        dropAllTables
      } else {
        clearAllTables
      }
    } catch {
      case e: Exception => println(s"Error on clean. Message: ${e.getMessage}")
    }
  }

  def init(force: Boolean): Unit = {
    try {
      val db = 'so
      DBs.setup(db)
      implicit val session: DBSession = NamedDB(db).autoCommitSession()

      createUserTable(force)
      createPostTable(force)
      Main.createCommentTable(force)
    } catch {
      case e: Exception => println(s"Error on init. Message: ${e.getMessage}")
    }
  }

  def extract(query: String, filePath: String): Unit = {
    try {
      val db = 'so
      DBs.setup(db)
      implicit val session: DBSession = NamedDB(db).readOnlySession()

      val lines = selectQueryToCsv(query)

      val bufferedWriter = Files.newBufferedWriter(Paths.get(filePath))
      val writer = new CSVWriter(bufferedWriter)
      lines.foreach(writer.writeNext(_, false))
      writer.close()
    } catch {
      case e: Exception => println(s"Error on extract. Message: ${e.getMessage}")
    }
  }

  def selectQueryToCsv(query: String)(implicit session: DBSession): Seq[Array[String]] = {
    var csvHeader: Array[String] = null
    val lines = session
      .list(query)(rs => {
        if (csvHeader == null) {
          csvHeader = 1.to(rs.metaData.getColumnCount)
            .map(rs.metaData.getColumnName)
            .toArray
        }

        1.to(rs.metaData.getColumnCount)
          .map(rs.string)
          .toArray
      })

    if (csvHeader == null) {
      lines
    } else {
      csvHeader +: lines
    }
  }

  def insertUsers(users: Seq[User])(implicit session: DBSession): Unit = {
    users.foreach(u => {
      try {
        sql"""
             insert into User(
                              id,
                              displayName,
                              location,
                              about,
                              reputation,
                              views,
                              upVotes,
                              downVotes,
                              accountId,
                              creationDate,
                              lastAccessDate
             )
             values(
                    ${u.id},
                    ${u.displayName},
                    ${u.location},
                    ${u.about},
                    ${u.reputation},
                    ${u.views},
                    ${u.upVotes},
                    ${u.downVotes},
                    ${u.accountId},
                    ${u.creationDate},
                    ${u.lastAccessDate}
             );""".update().apply()
      } catch {
        case e: SQLException => println(s"User with id=${u.id} was not inserted. Error message: ${e.getMessage}")
      }
    })
  }

  def insertPosts(posts: Seq[Post])(implicit session: DBSession): Unit = {
    posts.foreach(p => {
      try {
        sql"""
             insert into Post(
                              id,
                              title,
                              body,
                              score,
                              viewCount,
                              answerCount,
                              commentCount,
                              ownerUserId,
                              lastEditorUserId,
                              acceptedAnswerId,
                              creationDate,
                              lastEditDate,
                              lastActivityDate,
                              tags
             )
             values(
                    ${p.id},
                    ${p.title},
                    ${p.body},
                    ${p.score},
                    ${p.viewCount},
                    ${p.answerCount},
                    ${p.commentCount},
                    ${p.ownerUserId},
                    ${p.lastEditorUserId},
                    ${p.acceptedAnswerId},
                    ${p.creationDate},
                    ${p.lastEditDate},
                    ${p.lastActivityDate},
                    ${p.tags.mkString(", ")}
             );""".update().apply()
      } catch {
        case e: SQLException => println(s"Post with id=${p.id} was not inserted. Error message: ${e.getMessage}")
      }
    })
  }

  def insertComments(comments: Seq[Comment])(implicit session: DBSession): Unit = {
    comments.foreach(c => {
      try {
        sql"""
             insert into Comment(
                              id,
                              postId,
                              score,
                              text,
                              creationDate,
                              userId
             )
             values(
                    ${c.id},
                    ${c.postId},
                    ${c.score},
                    ${c.text},
                    ${c.creationDate},
                    ${c.userId}
             );""".update().apply()
      } catch {
        case e: SQLException => println(s"Comment with id=${c.id} was not inserted. Error message: ${e.getMessage}")
      }
    })
  }

  def clearAllTables(implicit session: DBSession): Unit = {
    clearUserTable
    clearPostTable
    clearCommentTable
  }

  def dropAllTables(implicit session: DBSession): Unit = {
    dropUserTable
    dropPostTable
    dropCommentTable
  }

  def createUserTable(force: Boolean = false)(implicit session: DBSession): Unit = {
    if (force) {
      dropUserTable
    }

    sql"""
         create table User(
             id int primary key,
             displayName varchar(100),
             location varchar(100),
             about varchar(6000),
             reputation int,
             views int,
             upVotes int,
             downVotes int,
             accountId int,
             creationDate date,
             lastAccessDate date
         );""".execute().apply()
  }

  def createPostTable(force: Boolean = false)(implicit session: DBSession): Unit = {
    if (force) {
      dropPostTable
    }

    sql"""
         create table Post(
             id int primary key,
             title varchar(400),
             body varchar(30000),
             score int,
             viewCount int,
             answerCount int,
             commentCount int,
             ownerUserId int,
             lastEditorUserId int,
             acceptedAnswerId int,
             creationDate date,
             lastEditDate date,
             lastActivityDate date,
             tags varchar(500)
         );""".execute().apply()
  }

  def createCommentTable(force: Boolean = false)(implicit session: DBSession): Unit = {
    if (force) {
      dropCommentTable
    }

    sql"""
           create table Comment(
               id int primary key,
               postId int references Post(id) on delete cascade,
               score int,
               text varchar(1000),
               creationDate date,
               userId int references User(id) on delete cascade
           );""".execute().apply()
  }

  def dropUserTable(implicit session: DBSession): Unit =
    sql"drop table User if exists;".execute().apply()

  def dropPostTable(implicit session: DBSession): Unit =
    sql"drop table Post if exists;".execute().apply()

  def dropCommentTable(implicit session: DBSession): Unit =
    sql"drop table Comment if exists;".execute().apply()

  def clearUserTable(implicit session: DBSession): Unit =
    sql"delete from User;".execute().apply()

  def clearPostTable(implicit session: DBSession): Unit =
    sql"delete from Post;".execute().apply()

  def clearCommentTable(implicit session: DBSession): Unit =
    sql"delete from Comment;".execute().apply()

}
