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

import java.util.UUID;

import be.nabu.eai.module.jdbc.pool.JDBCPoolArtifact;
import be.nabu.eai.module.jdbc.pool.api.JDBCPoolWizard;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.resources.RepositoryEntry;

public class H2FileWizard implements JDBCPoolWizard<H2FileProperties> {

	@Override
	public String getIcon() {
		return "h2-icon.png";
	}

	@Override
	public String getName() {
		return "H2 (File)";
	}

	@Override
	public Class<H2FileProperties> getWizardClass() {
		return H2FileProperties.class;
	}

	@Override
	public H2FileProperties load(JDBCPoolArtifact pool) {
		H2FileProperties properties = new H2FileProperties();
		String jdbcUrl = pool.getConfig().getJdbcUrl();
		if (jdbcUrl != null && jdbcUrl.startsWith("jdbc:h2:file:")) {
			String folder = jdbcUrl.substring("jdbc:h2:file:".length());
			// always show the path? or hide it if its the default?
//			if (!folder.equals("~/" + pool.getId())) {
				properties.setFolder(folder);
//			}
			return properties;
		}
		else {
			return null;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public JDBCPoolArtifact apply(Entry project, RepositoryEntry entry, H2FileProperties properties, boolean isNew, boolean isMain) {
		try {
			JDBCPoolArtifact existing = isNew ? new JDBCPoolArtifact(entry.getId(), entry.getContainer(), entry.getRepository()) : (JDBCPoolArtifact) entry.getNode().getArtifact();
			if (isNew) {
				existing.getConfig().setAutoCommit(false);
			}
			if (properties.getFolder() != null && !properties.getFolder().trim().isEmpty()) {
				existing.getConfig().setJdbcUrl("jdbc:h2:file:" + properties.getFolder());
			}
			else {
				existing.getConfig().setJdbcUrl("jdbc:h2:file:~/nabu/databases/" + entry.getId() + "-" + UUID.randomUUID().toString().replace("-", ""));
			}
			Class clazz = H2Dialect.class;
			existing.getConfig().setDialect(clazz);
			existing.getConfig().setDriverClassName("org.h2.Driver");
			return existing;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


}
