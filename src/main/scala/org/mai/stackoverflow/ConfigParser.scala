package org.mai.stackoverflow

import org.mai.stackoverflow.Commands.{Clean, Extract, Init, Load}
import scopt.OptionParser

object ConfigParser extends OptionParser[Config]("StackOverflowLoader") {
  head("StackOverflowLoader", "1.0")

  cmd("load")
    .text("Загрузка данных из файлов.")
    .action((_, config) => config.copy(command = Load))
    .children {
      opt[String]("path")
        .text("Путь к папке с файлами.")
        .action((value, config) => config.copy(path = value))
      opt[Unit]("append")
        .abbr("a")
        .text("Не удалять данные при загрузке. По умолчанию данные будут перезатираться.")
        .action((_, config) => config.copy(append = true))
    }

  cmd("clean")
    .text("Удаление данных из базы данных.")
    .action((_, config) => config.copy(command = Clean))
    .children {
      opt[Unit]("dropTables")
        .abbr("dt")
        .text("Удалить таблицы.")
        .action((_, config) => config.copy(dropTables = true))
    }

  cmd("init")
    .text("Создание таблиц.")
    .action((_, config) => config.copy(command = Init))
    .children {
      opt[Unit]("force")
        .abbr("f")
        .text("Пересоздать таблицы, если существуют.")
        .action((_, config) => config.copy(force = true))
    }

  cmd("extract")
    .text("Выгрузка данных в csv формате.")
    .action((_, config) => config.copy(command = Extract))
    .children {
      opt[String]("query")
        .abbr("q")
        .text("Запрос на выбор данных.")
        .action((value, config) => config.copy(query = value))
      opt[String]("file")
        .text("Файл, куда выгрузятся данные.")
        .action((value, config) => config.copy(file = value))
    }

  checkConfig(config => {
    config.command match {
      case null =>
        failure("Необходимо указать хотя бы одну команду.")
      case Load if config.path == null || config.path.isEmpty =>
        failure("Не указан путь к папке с файлами.")
      case Extract if config.query == null || config.query.isEmpty =>
        failure("Не указан запрос на выбор данных.")
      case Extract if config.file == null || config.file.isEmpty =>
        failure("Не указан файл для выгрузки данных.")
      case _ => success
    }
  })
}
