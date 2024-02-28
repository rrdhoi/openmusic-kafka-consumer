const { Pool } = require('pg');

class PlaylistSongsService {
  constructor() {
    this._pool = new Pool();
    this.tblPlaylists = 'playlists';
  }

  async getPlaylistSongs(playlistId) {
    const queryGetPlaylistById = {
      text: `SELECT p.id, p.name, u.username FROM ${this.tblPlaylists} AS p
         LEFT JOIN users AS u ON u.id = p.owner
         WHERE p.id = $1`,
      values: [playlistId],
    };

    const queryGetSongs = {
      text: `SELECT s.id, s.title, s.performer FROM songs AS s
         LEFT JOIN playlist_songs AS ps ON s.id = ps.song_id
         WHERE ps.playlist_id = $1
         GROUP BY s.id`,
      values: [playlistId],
    };

    const resultPlaylist = await this._pool.query(queryGetPlaylistById);
    const resultSongs = await this._pool.query(queryGetSongs);

    const finalResult = resultPlaylist.rows.map(({
      id,
      name,
    }) => ({
      id,
      name,
      songs: resultSongs.rows,
    }))[0];
    return {
      playlist: finalResult,
    };
  }
}

module.exports = PlaylistSongsService;
