# cascalog-workshop

An introduction to Cascalog.

## Get started

cascalog-workshop requires the following:

- Leiningen 2: [latest stable version](https://raw.github.com/technomancy/leiningen/stable/bin/lein)
- Hadoop 0.20.2: [tgz](http://archive.apache.org/dist/hadoop/core/hadoop-0.20.2/hadoop-0.20.2.tar.gz) and [md5sum](http://archive.apache.org/dist/hadoop/core/hadoop-0.20.2/hadoop-0.20.2.tar.gz.md5)

### Get Leiningen

If you do not already have Leiningen 2, you can download the latest
stable version from the repository on GitHub.

    wget https://raw.github.com/technomancy/leiningen/stable/bin/lein

Remember to make it executable.

    chmod +x lein

You may also want to add it to your `PATH`.

The first time you run `lein`, it will download its own dependencies and
bootstrap itself.

To exercise Leiningen, try running `lein marg` in the
`cascalog-workshop` project directory. This ought to run the
[`lein-marginalia`](https://github.com/fogus/lein-marginalia) plugin to
generate browseable documentation under the `docs/` directory.

### Get Hadoop

If you do not already have a working installation of Hadoop, you can
follow these instructions for a minimal setup.

1. Download the tgz archive for Hadoop version 0.20.2 and verify the md5 checksum.

        wget http://archive.apache.org/dist/hadoop/core/hadoop-0.20.2/hadoop-0.20.2{.tar.gz,.tar.gz.md5}
        md5sum -c hadoop-0.20.2.tar.gz.md5

2. Extract the archive and enter the destination directory.

        tar xfvz hadoop-0.20.2.tar.gz
        cd hadoop-0.20.2

3. Run the Hadoop executable script. This ought to print usage
   information and exit.

        bin/hadoop

4. Populate a folder with some input data for an example Hadoop job.

        mkdir /tmp/input
        cp bin/*.sh /tmp/input/

5. Run a job in non-distributed mode. This job reads the contents of the
   `/tmp/input/` directory as input, greps for `'hadoop'`, and writes
   output files to `/tmp/output`.

        bin/hadoop jar hadoop-0.20.2-examples.jar grep /tmp/input /tmp/output 'hadoop'

6. View the job output.

        cat /tmp/output/part-*

If you can run this example job, then your Hadoop setup is probably
ready for this workshop.

## License

Copyright © 2013 [Steve M. Kim](https://github.com/chairmanK)

Distributed under the Eclipse Public License, the same as Clojure.
