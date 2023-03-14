import spray.json._
import scala.annotation.tailrec

case class Artist(id: String, name: String, followers: Option[Int])

case class Song(name: String, duration_ms: Long, artists: Seq[Artist])

case class Item(track: Song)

case class PlaylistResponse(items: Seq[Item], limit: Int, total: Int, next: Option[String])

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val artistFormat: RootJsonFormat[Artist] = new RootJsonFormat[Artist] {
    override def write(obj: Artist): JsValue = {
      val baseMap = Map(
        "id" -> JsString(obj.id),
        "name" -> JsString(obj.name)
      )
      val followersMap = obj.followers.map(f => Map("followers" -> JsNumber(f))).getOrElse(Map.empty)
      JsObject(baseMap ++ followersMap)
    }

    def read(json: JsValue): Artist = {
      val fields = json.asJsObject.fields
      val id = fields("id").convertTo[String]
      val name = fields("name").convertTo[String]
      val followers = fields.get("followers").flatMap {
        case JsObject(followersFields) => followersFields.get("total").map(_.convertTo[Int])
        case _ => None
      }
      Artist(id, name, followers)
    }
  }
  implicit val trackFormat: RootJsonFormat[Song] = jsonFormat3(Song)
  implicit val itemFormat: RootJsonFormat[Item] = jsonFormat1(Item)
  implicit val playlistResponseFormat: RootJsonFormat[PlaylistResponse] = jsonFormat4(PlaylistResponse)
}

object SpotifyApi extends App {
  val token = "BQAU2BfbvNMpOoCzcxS_ImptOYoqY65o0jdsd-QnKXcRPHntKRdvKtrGt4jk7HzA11kOBmrvNVucpysQYM3azRxfjU3Otdktv4RSj7uzHt26DSWWhDcDz817E7iKKAB636oXu7IlMCMXUY_uqsU_16Nq8ZFZf1ztyZ8rDS1zSrgUJGhDv5jNkIBUetTptMf01oyho71QSzpJWr_zKBxnfry8iA7FREESH4FguZJPa_Nk-tGSDdxIPqIgBowCC3NUDpjRkG5059f_LsqT3DQkO6SuUCvoi6EA7k5QWf799EriPlkGC5yw4FWTduRuY4hwQhzKW6PHVGwnmA"
  val url = "https://api.spotify.com/v1/playlists/5Rrf7mqN8uus2AaQQQNdc1/tracks?offset=0&limit=100"
  val headers = Map(
    "Authorization" -> s"Bearer $token"
  )

  import MyJsonProtocol._

  @tailrec
  def getAllTracks(url: String, tracks: Seq[Song]): Seq[Song] = {
    val response = requests.get(url, headers = headers)
    val json = response.text.parseJson
    val playlistResponse = json.convertTo[PlaylistResponse]
    val allTracks = tracks ++ playlistResponse.items.map(_.track)

    if (playlistResponse.next.isDefined) {
      getAllTracks(playlistResponse.next.get, allTracks)
    } else {
      allTracks
    }
  }

  val allTracks = getAllTracks(url, Seq.empty)
  val top10LongestTracks = allTracks.sortWith(_.duration_ms > _.duration_ms).take(10)

  println("Top 10 tracks with longest duration: ")
  top10LongestTracks.foreach(track => println(s"Name: ${track.name}, Duration: ${track.duration_ms} ms"))


  val top10LongestTracksWithArtists = top10LongestTracks.map(track => {
    val artists = track.artists.map(artist => {
      val artistUrl = s"https://api.spotify.com/v1/artists/${artist.id}"
      val response = requests.get(artistUrl, headers = headers)
      val json = response.text.parseJson
      val artistObj = json.convertTo[Artist]
      (artistObj, track.duration_ms)
    })
    (track, artists)
  })


  // val sortedArtists = top10LongestTracksWithArtists.flatMap(_._2).map(_._1).distinct.sortBy(_.followers.getOrElse(0))(Ordering.Int.reverse)
  val artists = top10LongestTracksWithArtists
    .flatMap { case (_, artistList) => artistList.map { case (artist, _) => artist } }
    .distinct
  val sortedArtists = artists
    .sortBy { artist => artist.followers.getOrElse(0) }
    .reverse

  println("\nArtist name and follower count (sorted by decreasing order of followers) for the above tracks: ")
  sortedArtists.foreach(artist => println(s"Name: ${artist.name}, Followers: ${artist.followers.getOrElse(0)}"))
}