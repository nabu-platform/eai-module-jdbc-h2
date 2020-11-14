package be.nabu.eai.module.jdbc.dialects.h2;

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
			if (isMain) {
				existing.getConfig().setContext(project.getId());
			}
			if (properties.getFolder() != null && !properties.getFolder().trim().isEmpty()) {
				existing.getConfig().setJdbcUrl("jdbc:h2:file:" + properties.getFolder());
			}
			else {
				existing.getConfig().setJdbcUrl("jdbc:h2:file:~/" + entry.getId());
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
