package be.nabu.eai.module.jdbc.dialects.h2;

import be.nabu.libs.types.api.annotation.Field;

public class H2FileProperties {
	// can't make this a File because it is a path on the server...
	private String folder;

	@Field(comment = "The folder where the database should be stored. Note that this is a file path on the server, not your developer machine.", defaultValue = "Defaults to the home folder")
	public String getFolder() {
		return folder;
	}
	public void setFolder(String folder) {
		this.folder = folder;
	}
}
