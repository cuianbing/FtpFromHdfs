package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.ftplet.FileObject;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;

/**
 * Implemented FileSystemView to use HdfsFileObject
 */
public class HdfsFileSystemView implements FileSystemView {

	// the root directory will always end with '/'.
	private String rootDir = "/";

	// 第一个和最后一个字符始终是'/'
	// 绝对路径(总是关于根目录的)
	private String currDir = "/";

	private User user;

	// private boolean writePermission;

	private boolean caseInsensitive = false;

	/**
	 * Constructor - set the user object.
	 */
	protected HdfsFileSystemView(User user) throws FtpException {
		this(user, true);
	}

	/**
	 * Constructor - set the user object.
	 */
	protected HdfsFileSystemView(User user, boolean caseInsensitive)
			throws FtpException {
		if (user == null) {
			throw new IllegalArgumentException("user can not be null");
		}
		if (user.getHomeDirectory() == null) {
			throw new IllegalArgumentException(
					"User home directory can not be null");
		}

		this.setCaseInsensitive(caseInsensitive);

		// add last '/' if necessary 如果有必要，添加最后一个'/'
		String rootDir = user.getHomeDirectory();
		//  rootDir = NativeFileObject.normalizeSeparateChar(rootDir);
		if (!rootDir.endsWith("/")) {
			rootDir += '/';
		}
		this.setRootDir(rootDir);

		this.user = user;


	}

	/**
	 * Get the user home directory. It would be the file system root for the
	 * user.
	 */
	public FileObject getHomeDirectory() {
		return new HdfsFileObject("/", user);
	}

	/**
	 * Get the current directory.
	 */
	public FileObject getCurrentDirectory() {
		return new HdfsFileObject(currDir, user);
	}

	/**
	 * Get file object.
	 */
	public FileObject getFileObject(String file) {
		String path;
		if (file.startsWith("/")) {
			path = file;
		} else if (currDir.length() > 1) {
			path = currDir + "/" + file;
		} else {
			path = "/" + file;
		}
		return new HdfsFileObject(path, user);
	}

	/**
	 * Change directory. 改变当前工作目录；改变当前的路径；
	 */
	public boolean changeDirectory(String dir) {
		String path;
		if (dir.startsWith("/")) {
			path = dir;
		} else if (currDir.length() > 1) {
			path = currDir + "/" + dir;
		} else {
			path = "/" + dir;
		}
		HdfsFileObject file = new HdfsFileObject(path, user);
		if (file.isDirectory() && file.hasReadPermission()) {
			currDir = path;
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Is the file content random accessible? 
	 * 文件内容是否可随意访问?
	 */
	public boolean isRandomAccessible() {
		return true;
	}

	/**
	 * Dispose file system view - does nothing. 
	 * 处理文件系统视图-什么也不做。
	 */
	public void dispose() {
	}

	
	/**********************/
	public String getRootDir() {
		return rootDir;
	}

	public void setRootDir(String rootDir) {
		this.rootDir = rootDir;
	}

	public boolean isCaseInsensitive() {
		return caseInsensitive;
	}

	public void setCaseInsensitive(boolean caseInsensitive) {
		this.caseInsensitive = caseInsensitive;
	}
}
