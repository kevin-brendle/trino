<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:sm="http://www.sitemaps.org/schemas/sitemap/0.9">
    <xsl:template match="sm:urlset">
        <xsl:copy>
            <xsl:apply-templates select="@*"/>
            <xsl:apply-templates select="sm:url">
                <xsl:sort select="sm:loc" order="ascending"/>
            </xsl:apply-templates>
        </xsl:copy>
    </xsl:template>
    <xsl:template match="@* | node()">
        <xsl:copy>
            <xsl:apply-templates select="@* | node()"/>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>
