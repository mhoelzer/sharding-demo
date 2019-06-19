import os
import json
from shutil import copyfile

filename = "chapter2.txt"


def load_data_from_file(path=None):
    with open(path if path else filename, 'r') as f:
        data = f.read()
    return data


class ShardHandler(object):
    """
    Take any text file and shard it into X number of files with
    Y number of replications.
    """

    def __init__(self):
        self.mapping = self.load_map()

    mapfile = "mapping.json"

    def write_map(self):
        """Write the current 'database' mapping to file."""
        with open(self.mapfile, 'w') as m:
            json.dump(self.mapping, m, indent=2)

    def load_map(self):
        """Load the 'database' mapping from file."""
        if not os.path.exists(self.mapfile):
            return dict()
        # read the mapping.json and resturn as dict
        with open(self.mapfile, 'r') as m:
            return json.load(m)

    def build_shards(self, count, data=None):
        """Initialize our miniature databases from a clean mapfile. Cannot
        be called if there is an existing mapping -- must use add_shard() or
        remove_shard()."""
        if self.mapping != {}:
            return "Cannot build shard setup -- sharding already exists."

        spliced_data = self._generate_sharded_data(count, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

    def _write_shard(self, num, data):
        """Write an individual database shard to disk and add it to the
        mapping."""
        if not os.path.exists("data"):
            os.mkdir("data")
        with open(f"data/{num}.txt", 'w') as s:
            s.write(data)

        # the char from before; 1 shard at a time
        # map telsl wehre data is, but need to have indexes to find teh stuff
        # this is horz
        self.mapping.update(
            {
                str(num): {
                    'start': num * len(data),
                    'end': (num + 1) * len(data)
                }
            }
        )

    def _generate_sharded_data(self, count, data):
        """Split the data into as many pieces as needed."""
        # divmod = takes int and another arg and to get x num things, first arg reurne dis where we need to split stuff, and rem is ermainder if stuff left over; tells index num, not stuff with data
        splicenum, rem = divmod(len(data), count)
        # data is string of w/e; here's text
        result = [
            data[
                splicenum * z:  # if z is 0, it's 0; takes text and puts into segments
                splicenum * (z + 1)  # 25 * 0+1
            ] for z in range(count)
        ]
        # result = 0-25; 25-50;...
        # text blocks in thign
        # take care of any odd characters
        # if no remainder, then all is good; else ...
        if rem > 0:
            # result last += bigblob[countBackwardsToEndJustThoseCharAndThrowEverythingElse]
            # -1:issecondtolast
            result[-1] += data[-rem:]

        return result  # list of data split along index

    def load_data_from_shards(self):
        """Grab all the shards, pull all the data, and then concatenate it."""
        result = list()
        # looks at the keys inside the dict from load_maps
        for db in self.mapping.keys():
            with open(f'data/{db}.txt', 'r') as f:
                # appends the files to the list
                result.append(f.read())
        return ''.join(result)  # everything writern; string

    def add_shard(self):
        """Add a new shard to the existing pool and rebalance the data."""
        self.mapping = self.load_map()
        data = self.load_data_from_shards()
        # figureout howmany you have and what to do next; map gets desynced, we lose data
        # recast list of strings to int to sort
        keys = [int(z) for z in list(self.mapping.keys())]
        keys.sort()
        # why 2? Because we have to compensate for zero indexing
        # looks for highest val element, which will always be nine w/o making int; like aa comes before b
        new_shard_num = str(max(keys) + 2)
        # all org data split how much we want; evenly divided final list
        spliced_data = self._generate_sharded_data(int(new_shard_num), data)

        # en = looping stuff; s_d is elemetns; list of 4 num, prritns 4 things inside list; gives us loop num that we're on, which does count num of loop we're on; num loop and elemnt for loop; loop 0 gives el 1; extra piece of data in loop and go through each elemnt and says what itersation
        # writes our files; file 0 is same as loop 0 and stores first el in arrya
        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)
            # end up with updated dic and stuff written to disk in data folder; split u; mappign is ready

        self.write_map()  # backup index

    def remove_shard(self):
        """Loads the data from all shards, removes the extra 'database' file,
        and writes the new number of shards to disk.
        """
        # take in data, rebalance for fewer num, write that to disk, and end up with a little more data in shards
        # like add shard load all dataset; we grab all and hold and rewrite

        # is the dic with
        self.mapping = self.load_map()
        data = self.load_data_from_shards()

        keys = [int(z) for z in list(self.mapping.keys())]
        keys.sort()
        new_shard_num = str(max(keys))

        spliced_data = self._generate_sharded_data(int(new_shard_num), data)

        # shows what shard; for 0, string in
        for num, d in enumerate(spliced_data):
            # based on mapping and pick that nu
            self._write_shard(num, d)

        try:
            os.remove(f'data/{new_shard_num}.txt')
            self.mapping.pop(new_shard_num)
        except ZeroDivisionError:
            pass

        self.write_map()
        # if 1 left, stop

    def existing_shard_level(self):
        # secondary func for add and remove to show existing level; cant assume that there is or not rep; can call how many times; know highest num b/c that's what to remove and use btoh here and rem; fault tolerante (sync rrep verifies stuff is all there; poll all for highest)
        # find first file and go up when doing loop below; copmare old 
        files = os.listdir("data")
        # zero_file = str(files).startswith("0")
        # first_file = list(filter(zero_file, files))
        # looking at lowest level file that'll always be there to check with hlr to see rep levels on that file
        lowest_level_file = list(filter(lambda file: file.startswith("0"), files))
        highest_level_reps = [int(file.split("-")[1].replace(".txt", "")) for file in lowest_level_file if "-" in file]
        # for file in lowest_level_file:
        #     if "-" in file:
        #         highest_level_reps = int(file.split("-")[1][:-4])
        #         return list(highest_level_reps)
        if highest_level_reps:
            return max(highest_level_reps)
        else:
            return 0
    
    def add_replication(self):
        """Add a level of replication so that each shard has a backup. Label
        them with the following format:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        2-1.txt (shard 2, replication 1)
        ...etc.

        By default, there is no replication -- add_replication should be able
        to detect how many levels there are and appropriately add the next
        level.
        """
        # get the individual shards; for each,  reat a copy with a title that starts with - and then a num
        # identify level and copy it appropriately
        # write data to primaries, so check to see if prims good ans backwd; syncing; figure out early on since epxensize for time/$
        # cold backup = must be rresotrred and stored to disk but not curr running; hot is curr running
        
        copy_level = self.existing_shard_level()
        self.mapping = self.load_map()
        for key in self.mapping.keys():
            original = f'data/{key}.txt'
            replication = f'data/{key}-{str(copy_level + 1)}.txt'
            copyfile(original, replication)

    def remove_replication(self):
        """Remove the highest replication level.

        If there are only primary files left, remove_replication should raise
        an exception stating that there is nothing left to remove.

        For example:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        etc...

        to:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        2.txt (shard 2, primary)
        etc...
        """
        # removes highest level, like 1-3 is gone; exception if no dups left
        pass

    def sync_replication(self):
        """Verify that all replications are equal to their primaries and that
         any missing primaries are appropriately recreated from their
         replications."""
        #  if primary/main isn't there, finds that a primary is gone and restore by copying a replication file
        # balance data wehn add new shards and stuff; file structure should match metadata; num of rreps is good and elvel nm is good
        pass

    def get_shard_data(self, shardnum=None):
        """Return information about a shard from the mapfile."""
        if not shardnum:
            return self.get_all_shard_data()
        data = self.mapping.get(shardnum)
        if not data:
            return f"Invalid shard ID. Valid shard IDs: {self.mapping.keys()}"
        return f"Shard {shardnum}: {data}"

    def get_all_shard_data(self):
        """A helper function to view the mapping data."""
        return self.mapping


s = ShardHandler()

s.build_shards(5, load_data_from_file())

print(s.mapping.keys())

# s.add_shard()
# s.remove_shard()
s.add_replication()

print(s.mapping.keys())
