package simpledb.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileUtil {
    public static byte[] readContent(File file, int offset, int len) throws IOException {
        byte[] buffer = new byte[len];
        long fileSize = file.length();
        if (fileSize > Integer.MAX_VALUE) {
            throw new IOException(String.format("文件太大 size = %d", fileSize));
        }
        RandomAccessFile fi = new RandomAccessFile(file, "r");
        fi.seek(offset);
        int readNum = fi.read(buffer, 0, len);
        if (readNum != buffer.length) {
            throw new IOException("没有读取完整文件");
        }
        fi.close();
        return buffer;
    }

    public static void writeContent(File file, int offset, byte[] content) throws IOException {
        RandomAccessFile fi = new RandomAccessFile(file, "rw");
        fi.seek(offset);
        fi.write(content);
        fi.close();
    }

}
