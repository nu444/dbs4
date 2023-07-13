import de.hpi.dbs1.ChosenImplementation
import de.hpi.dbs1.ConnectionConfig
import de.hpi.dbs1.JDBCExercise
import de.hpi.dbs1.entities.Actor
import de.hpi.dbs1.entities.Movie
import java.sql.Connection
import java.sql.DriverManager
import java.util.logging.Logger

@ChosenImplementation(true)
class JDBCExerciseKotlinImplementation : JDBCExercise {

    val logger = Logger.getLogger(javaClass.simpleName)

    override fun createConnection(config: ConnectionConfig): Connection {
        val url = "jdbc:postgresql://localhost:5432/IMDB"
        return DriverManager.getConnection(url, "postgres", "12345")
    }

    override fun queryMovies(
        connection: Connection,
        keywords: String
    ): List<Movie> {
        logger.info(keywords)
        val movies = ArrayList<Movie>()

        /*
        val myMovie = Movie("??????????", "My Movie", 2023, setOf("Indie"))
        myMovie.actorNames.add("Myself")
        movies.add(myMovie)
        */

        val stmt = connection.prepareStatement(
            "SELECT * FROM tmovies WHERE title LIKE '%$keywords%' ORDER BY title NULLS LAST, year")
        val actStmt = connection.prepareStatement("SELECT primaryname FROM tprincipals NATURAL JOIN nbasics WHERE tconst = ? AND (category = 'actor' OR category = 'actress') ORDER BY primaryname")
        //stmt.setString(1, keywords)
        val rs = stmt.executeQuery()

        val movieList = ArrayList<Movie>()

        while(rs.next()){
            val g = rs.getArray("genres")
            val genres = g.getArray() as Array<String>
            val newMovie = Movie(
                rs.getString("tconst"),
                rs.getString("title"),
                rs.getInt("year"),
                genres.toSet()
            )
            actStmt.setString(1, rs.getString("tconst"))
            val actNames = actStmt.executeQuery();
            while (actNames.next()){
                newMovie.actorNames.add(actNames.getString("primaryname"))
            }
            movieList.add(newMovie)
        }

        return movieList
    }

    override fun queryActors(
        connection: Connection,
        keywords: String
    ): List<Actor> {
        logger.info(keywords)
        val actors = ArrayList<Actor>()
        val stmt = connection.createStatement()
        val actorRes = stmt.executeQuery("SELECT nconst, primaryname, COUNT(*) FROM tprincipals NATURAL JOIN nbasics WHERE primaryname LIKE '%$keywords%' AND (category = 'actor' OR category = 'actress') GROUP BY nconst, primaryname ORDER BY count DESC, primaryname LIMIT 5")
        val mStmt = connection.prepareStatement("SELECT title FROM tprincipals NATURAL JOIN tmovies WHERE (nconst = ?) AND (category = 'actor' OR category = 'actress') ORDER BY year DESC NULLS LAST, title LIMIT 5")
        while (actorRes.next()){
            val newActor = Actor(
                actorRes.getString("nconst"),
                actorRes.getString("primaryname")
            )
            mStmt.setString(1, newActor.nConst)
            val mRes = mStmt.executeQuery()
            while (mRes.next()){
                newActor.playedIn.add(mRes.getString("title"))
            }
            actors.add(newActor)
        }
        return actors

    }
}
