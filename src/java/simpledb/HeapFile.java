package simpledb;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
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
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile file = new RandomAccessFile(this.f, "r");
            byte[] b = new byte[BufferPool.getPageSize()];
            file.seek(((long) BufferPool.getPageSize() * pid.getPageNumber()));
            file.read(b);
            file.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), b);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException f) {
            f.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile file = new RandomAccessFile(this.f, "rw");
        file.seek(((long) BufferPool.getPageSize() * page.getId().getPageNumber()));
        file.write(page.getPageData());
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (this.f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> list = new ArrayList<>();
        for (int pno = 0; pno < this.numPages(); pno++) {
            PageId pid = new HeapPageId(this.getId(), pno);
            HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (hp.getNumEmptySlots() > 0) {
                hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                hp.insertTuple(t);
                list.add(hp);
                break;
            } else {
                Database.getBufferPool().releasePage(tid, pid);
            }
        }
        if (list.isEmpty()) {
            HeapPage p = new HeapPage(new HeapPageId(this.getId(), this.numPages()), new byte[BufferPool.getPageSize()]);
            p.insertTuple(t);
            this.writePage(p);
            list.add(p);
        }
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();

        HeapPage deletion = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        deletion.deleteTuple(t);

        ArrayList<Page> list = new ArrayList<>();
        list.add(deletion);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

    public class HeapFileIterator implements DbFileIterator {

        private boolean opened;
        private HeapPage currPage;
        private int currNo;
        private TransactionId tid;
        private HeapFile file;
        private Iterator<Tuple> currIterator;

        public HeapFileIterator(TransactionId tid, HeapFile file) {
            this.opened = false;
            this.tid = tid;
            this.file = file;
        }

        private void setCurrPage() throws TransactionAbortedException, DbException {
            this.currPage = (HeapPage) Database.getBufferPool().getPage(this.tid,
                    new HeapPageId(file.getId(), this.currNo), Permissions.READ_ONLY);
            this.currIterator = this.currPage.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.opened = true;
            this.currNo = 0;
            setCurrPage();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.opened) {
                if (this.currIterator.hasNext()) {
                    return true;
                } else {
                    while (this.currNo < file.numPages()-1) {
                        this.currNo++;
                        setCurrPage();
                        if (this.currIterator.hasNext()) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
           if (this.hasNext()) {
               return this.currIterator.next();
           } else {
               throw new NoSuchElementException("No more tuples could be found");
           }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.currNo = 0;
            setCurrPage();
        }

        @Override
        public void close() {
            this.currPage = null;
            this.currIterator = null;
            this.opened = false;
        }
    }

}

