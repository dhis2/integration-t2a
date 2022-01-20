package org.hisp.dhis.integration.t2a.model;

public class Dimensions {

    private final String period;
    private final OrganisationUnit organisationUnit;
    private final ProgramIndicator programIndicator;

    public Dimensions(String period, OrganisationUnit organisationUnit, ProgramIndicator programIndicator) {
        this.period = period;
        this.organisationUnit = organisationUnit;
        this.programIndicator = programIndicator;
    }

    public OrganisationUnit getOrganisationUnit() {
        return organisationUnit;
    }

    public ProgramIndicator getProgramIndicator() {
        return programIndicator;
    }

    public String getPeriod() {
        return period;
    }
}
