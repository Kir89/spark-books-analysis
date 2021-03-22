package it.kirsoft.examples.booksanalysisspark.config

/**
 * Contains the DB configuration of the Spark Application
 * @param url url of the DB (contains DB name too)
 * @param dbTable Name of the Table you want to use
 * @param username to connect to the DB
 * @param password of the username
 */
case class DBConfig(url: String, dbTable: String, username: String, password: String)
