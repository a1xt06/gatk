package org.broadinstitute.hellbender.tools.spark.sv.utils;

import htsjdk.samtools.*;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;

public final class FileUtils {

    public static void writeSAMFile(final Iterator<SAMRecord> alignments, final SAMFileHeader header, final String outputName,
                             final boolean preOrdered) {
        try ( final SAMFileWriter writer = createSAMFileWriter(outputName, header, preOrdered) ) {
            alignments.forEachRemaining(writer::addAlignment);
        } catch ( final UncheckedIOException ie) {
            throw new GATKException("Can't write SAM file to the specified location: " + outputName, ie);
        }
    }

    private static SAMFileWriter createSAMFileWriter(final String samFile, final SAMFileHeader header, final boolean preOrdered) {
        final SAMFileWriterFactory factory = new SAMFileWriterFactory();
        final int lastDotIndex = samFile.lastIndexOf('.');
        if (lastDotIndex >= 0) {
            final String extension = samFile.substring(lastDotIndex).toLowerCase();
            if (extension.equals(BamFileIoUtils.BAM_FILE_EXTENSION)) {
                return factory.makeBAMWriter(header, preOrdered, BucketUtils.createFile(samFile));
            } else if (extension.equals(".sam")) {
                return factory.makeSAMWriter(header, preOrdered, BucketUtils.createFile(samFile));
            } else {
                throw new GATKException("unsupported read alignment file name extension (." + extension + ") in requested name: " + samFile);
            }
        } else {
            throw new GATKException("cannot determine the alignment file format from its name: " + samFile);
        }
    }

    public static void writeLinesToSingleFile(final Iterator<String> linesToWrite, final String fileName) {
        try ( final OutputStream writer =
                      new BufferedOutputStream(BucketUtils.createFile(fileName)) ) {
            while (linesToWrite.hasNext()) {
                writer.write(linesToWrite.next().getBytes()); writer.write('\n');
            }
        } catch ( final IOException ioe ) {
            throw new GATKException("Can't write "+fileName, ioe);
        }
    }

    public static boolean createDirInBucketToWriteTo(final String pathString) {
        try {
            Utils.nonNull(pathString);
            if ( java.nio.file.Files.exists(java.nio.file.Paths.get(pathString)) )
                throw new IOException("Directory to be created already exists: " + pathString);

            final boolean isSuccessful;
            if (BucketUtils.isCloudStorageUrl(pathString)) {
                final java.nio.file.Path dir = java.nio.file.Files.createDirectory(BucketUtils.getPathOnGcs(pathString));
                isSuccessful = java.nio.file.Files.isDirectory(dir) && java.nio.file.Files.isWritable(dir);
            } else if (BucketUtils.isHadoopUrl(pathString)) {
                isSuccessful = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration()).mkdirs(new org.apache.hadoop.fs.Path(pathString));
            } else {
                final java.nio.file.Path dir = java.nio.file.Files.createDirectory( java.nio.file.Paths.get(pathString) );
                isSuccessful = java.nio.file.Files.isDirectory(dir) && java.nio.file.Files.isWritable(dir);
            }
            return isSuccessful;
        } catch (final IOException x) {
            throw new GATKException("Could not create directory: " + pathString, x);
        }
    }
}
