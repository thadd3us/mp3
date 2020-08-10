"""Renames (and moves) MP3s based on data in ID3 tags.

Musicbrainz Picard wasn't able to use the "genre" tag to rename things, so I wrote this for myself.

Tries to be careful to not destroy/clobber anything.  No warranties of any kind.

Useful:
cat collisions.txt | while IFS= read -r f; do mp3info -x -F -rm "$f" -p "%F\t%r\n" ; done

Author: Thad Hughes (thad@thadhughes.com)
"""
import collections
import logging
import os
import re
import sys

import pandas as pd
from absl import app
from absl import flags
from mutagen import easyid3

FLAGS = flags.FLAGS
flags.DEFINE_string('input_mp3_dir', None, 'Where look for MP3s, recursively.')
flags.DEFINE_string('output_mp3_base_dir', None, 'Where to write MP3s.  Can be the same as input_mp3_dir')
flags.DEFINE_bool('dry_run', True, '.')

DATE_TO_YEAR = re.compile(r'([12][0-9][0-9][0-9])-[0-1][0-9]-[0-3][0-9]')
PUNCTUATION = re.compile(r'''[:!@#$%^&*/\\?<>"]''')
MAX_NAME_LENGTH = 60

def normalize_name(name: str, missing_value) -> str:
  if not name:
    return missing_value
  name = PUNCTUATION.sub('_', name)
  if len(name) > MAX_NAME_LENGTH:
    name = name[0:MAX_NAME_LENGTH]
  return name


def process_mp3(root, item, dest_dir_base, records):
  try:
    filename = os.path.join(root, item)
    f = easyid3.EasyID3(filename)
    record = collections.OrderedDict()
    record['original_filename'] = filename

    genre = f.get('genre', ['UNKNOWN_GENRE'])[0]

    artistsort = f.get('artistsort', [None])[0]
    artist = f.get('artist', [None])[0]
    albumartist = f.get('albumartist', [None])[0]
    if not artistsort:
      artistsort = artist

    year = f.get('date', [''])[0]
    m = DATE_TO_YEAR.match(year)
    if m:
      year = m.group(1)
    if not year:
      year = f.get('year', ['UNKNOWN_YEAR'])[0]

    album = f.get('album', ['UNKNOWN_ALBUM'])[0]

    track_string = 'UNKNOWN_TRACK'
    track = f.get('tracknumber', [None])[0]
    if track:
      track_split = track.split('/')
      track_number = int(track_split[0])
      track_width = len(track_split[1])  # TODO: use this
      track_string = f'{track_number:02}'
    # TODO: handle disk number

    title = f.get('title', ['UNKNOWN_TITLE'])[0]

    genre = normalize_name(genre, 'UNKNOWN_GENRE')
    artist = normalize_name(artist, 'UNKNOWN_ARTIST')
    artistsort = normalize_name(artistsort, 'UNKNOWN_ARTISTSORT')
    albumartist = normalize_name(albumartist, None)
    year = normalize_name(year, 'UNKNOWN_YEAR')
    album = normalize_name(album, 'UNKNOWN_ALBUM')
    track_string = normalize_name(track_string, 'UNKNOWN_TRACK')
    title = normalize_name(title, 'UNKNOWN_TITLE')

    if albumartist:
      new_name = f'{genre}/{albumartist}/{year} - {album}/{album} - {year} - {track_string} - {artist} - {title}.mp3'
    else:
      new_name = f'{genre}/{artistsort}/{year} - {album}/{artist} - {year} - {album} - {track_string} - {title}.mp3'

    new_filename = os.path.join(dest_dir_base, new_name)
    record['new_filename'] = new_filename
    name_changed = new_filename != filename
    record['name_changed'] = name_changed

    if name_changed and os.path.exists(new_filename):
      record['collision'] = 'PREEXISTING FILE'
    else:
      record['collision'] = None

    record.update(f)
    records.append(record)
  except Exception as e:
    logging.error('Failure processing file: %s, tags=%s', filename, f, exc_info=e)


def process_mp3s(input_mp3_dir: str, output_mp3_base_dir: str, dry_run: bool):
  records = []
  for root, _, files in os.walk(input_mp3_dir):
    for item in files:
      if item.lower().endswith('mp3'):
        process_mp3(root, item, FLAGS.output_mp3_base_dir, records)
  df = pd.DataFrame.from_records(records)
  df = df.sort_values('new_filename')

  preexisting_collisions = df.new_filename[~pd.isna(df.collision)]
  if len(preexisting_collisions.index) > 0:
    logging.error('Pre-existing collisions, stopping:\n%s', preexisting_collisions.values)
    return

  new_filename_counts = df.new_filename.value_counts()
  collisions = new_filename_counts[new_filename_counts > 1]
  if len(collisions.index) > 0:
    logging.error('Would-be new collisions, stopping.')
    collisions_df = df[df.new_filename.isin(collisions.index)]
    print(collisions_df[['original_filename']].to_csv(index=False, header=False, sep='\t'))
    return

  df = df[df.name_changed]
  for _, row in df.iterrows():
    assert '"' not in row.original_filename
    assert '"' not in row.new_filename
    print('mv "%s" "%s"' % (row.original_filename, row.new_filename))
    assert not os.path.exists(row.new_filename), row.new_filename
    if not dry_run:
      os.makedirs(os.path.dirname(row.new_filename), exist_ok=True)
      os.rename(row.original_filename, row.new_filename)


def main(argv):
  logging.basicConfig(stream=sys.stderr, level=logging.INFO)

  if len(argv) > 1:
    logging.warning('Unparsed arguments: %s', argv)
  process_mp3s(FLAGS.input_mp3_dir, FLAGS.output_mp3_base_dir, FLAGS.dry_run)


if __name__ == '__main__':
  flags.mark_flags_as_required(['input_mp3_dir', 'output_mp3_base_dir'])
  app.run(main)
