package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        if (f == null || td == null) {
            throw new IllegalArgumentException("File and TupleDesc cannot be null");
        }
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (!(pid instanceof HeapPageId)) {
            throw new IllegalArgumentException("PageId must be an instance of HeapPageId");
        }
        HeapPageId heapPageId = (HeapPageId) pid;
        if (heapPageId.getTableId() != getId()) {
            throw new IllegalArgumentException("PageId does not belong to this HeapFile");
        }   
        int pageSize = BufferPool.getPageSize();
        int pageNumber = heapPageId.getPageNumber();
        int offset = pageNumber * pageSize;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            if (offset + pageSize > raf.length()) { 
                throw new IllegalArgumentException("page number out of range");
            }
            raf.seek(offset);
            byte[] data = new byte[pageSize];
            raf.readFully(data);

            return new HeapPage(heapPageId, data);
        } catch (IOException e) {
            throw new IllegalStateException("Error reading page from file: " + e.getMessage(), e);
        }
    }
    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long fileSize = file.length();
        int pageSize = BufferPool.getPageSize();
        return (int) (fileSize / pageSize);
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int currentPageIndex = 0;
            private Iterator<Tuple> tupleIter;
            private boolean open = false;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                open = true;
                currentPageIndex = 0;
                loadPageIterator();
            }

            private void loadPageIterator() throws DbException, TransactionAbortedException {
                if (currentPageIndex < numPages()) {
                    HeapPageId pid = new HeapPageId(getId(), currentPageIndex);
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    tupleIter = page.iterator();
                }else {
                    tupleIter = null;
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!open || tupleIter == null) {
                    return false;
                }
                while (!tupleIter.hasNext()) {
                    currentPageIndex++;
                    if (currentPageIndex >= numPages()) {
                        return false;
                    }
                    loadPageIterator();
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more tuples in the iterator");
                }
                return tupleIter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                open = false;
                tupleIter = null;
            }
        };
    }
}

