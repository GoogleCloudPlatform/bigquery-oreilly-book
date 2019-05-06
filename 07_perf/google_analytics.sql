SELECT DISTINCT
 visitId
 , totals.pageviews
 , totals.timeOnsite
 , trafficSource.source
 , device.browser
 , device.isMobile
 , h.page.pageTitle
FROM 
  `bigquery-public-data`.google_analytics_sample.ga_sessions_20170801,
  UNNEST(hits) AS h 
WHERE
  totals.timeOnSite IS NOT NULL AND h.page.pageTitle = 'Shopping Cart'
ORDER BY pageviews DESC
LIMIT 10

