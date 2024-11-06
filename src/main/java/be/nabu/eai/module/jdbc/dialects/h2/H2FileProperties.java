/*
* Copyright (C) 2019 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.eai.module.jdbc.dialects.h2;

import be.nabu.libs.types.api.annotation.Field;

public class H2FileProperties {
	// can't make this a File because it is a path on the server...
	private String folder;

	@Field(comment = "The folder where the database should be stored. Note that this is a file path on the server, not your developer machine.\n\nThe default value should be enough for most cases so this can usually be left empty. Note that file-based h2 databases should not be used in clustered setups.", defaultValue = "Defaults to the home folder")
	public String getFolder() {
		return folder;
	}
	public void setFolder(String folder) {
		this.folder = folder;
	}
}
