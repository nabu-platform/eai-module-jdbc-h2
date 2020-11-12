package be.nabu.eai.module.jdbc.dialects.h2;

import be.nabu.eai.module.jdbc.pool.api.JDBCPoolWizard;
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
	public H2FileProperties load(RepositoryEntry entry) {
		return new H2FileProperties();
	}

	@Override
	public void save(H2FileProperties instance, RepositoryEntry entry) {
		// do nothing!
	}

}
