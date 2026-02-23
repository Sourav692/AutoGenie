import "./app.css";
import { useMemo, useState } from "react";
import {
  useAnalyticsQuery,
  Skeleton,
  BarChart,
  LineChart,
  DonutChart,
  DataTable,
  Tabs,
  TabsList,
  TabsTrigger,
  TabsContent,
  Card,
  CardHeader,
  CardTitle,
  CardContent,
  Badge,
  Alert,
} from "@databricks/appkit-ui/react";
import { toNumber, formatCurrency, formatNumber } from "../../shared/types";

const CONFIG = {
  catalog: "vc_catalog",
  schema: "aibi_sales_pipeline_review",
  tables: ["accounts", "opportunity", "opportunityhistory_cube", "user"],
  warehouseId: "862f1d757f0424f7",
  lookbackDays: 90,
  confidenceThreshold: 0.75,
};

const BRAND_COLORS = ["#40d1f5", "#4462c9", "#EB1600", "#0B2026", "#4A4A4A", "#353a4a"];

function KpiCard({
  title,
  value,
  subtitle,
  accent,
}: {
  title: string;
  value: string;
  subtitle?: string;
  accent?: string;
}) {
  return (
    <div className="kpi-card" style={{ borderTopColor: accent ?? BRAND_COLORS[0] }}>
      <div className="kpi-label">{title}</div>
      <div className="kpi-value">{value}</div>
      {subtitle && <div className="kpi-subtitle">{subtitle}</div>}
    </div>
  );
}

function KpiSection() {
  const params = useMemo(() => ({}), []);
  const { data, loading, error } = useAnalyticsQuery("pipeline_kpis", params);

  if (loading) {
    return (
      <div className="kpi-grid">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-28 rounded-xl" />
        ))}
      </div>
    );
  }
  if (error) return <Alert variant="destructive">Error loading KPIs: {error}</Alert>;
  if (!data || data.length === 0) return null;

  const row = data[0];
  const winRate =
    toNumber(row.closed_won) + toNumber(row.closed_lost) > 0
      ? ((toNumber(row.closed_won) / (toNumber(row.closed_won) + toNumber(row.closed_lost))) * 100).toFixed(1)
      : "0";

  return (
    <div className="kpi-grid">
      <KpiCard
        title="Total Pipeline"
        value={formatCurrency(row.total_pipeline)}
        subtitle={`${formatNumber(row.total_opportunities)} deals`}
        accent={BRAND_COLORS[0]}
      />
      <KpiCard
        title="Open Pipeline"
        value={formatCurrency(row.open_pipeline)}
        subtitle="Excluding closed deals"
        accent={BRAND_COLORS[1]}
      />
      <KpiCard
        title="Weighted Pipeline"
        value={formatCurrency(row.weighted_pipeline)}
        subtitle="Probability-adjusted"
        accent="#10b981"
      />
      <KpiCard
        title="Win Rate"
        value={`${winRate}%`}
        subtitle={`${formatNumber(row.closed_won)} won / ${formatNumber(row.closed_lost)} lost`}
        accent="#f59e0b"
      />
    </div>
  );
}

function TableOverview() {
  const params = useMemo(() => ({}), []);
  const { data, loading, error } = useAnalyticsQuery("table_summary", params);

  if (loading) return <Skeleton className="h-32 rounded-xl" />;
  if (error) return <Alert variant="destructive">Error: {error}</Alert>;
  if (!data) return null;

  return (
    <div className="table-overview-grid">
      {data.map((row) => (
        <div key={row.table_name} className="table-overview-item">
          <div className="table-overview-name">{row.table_name}</div>
          <div className="table-overview-count">{formatNumber(row.row_count)}</div>
          <div className="table-overview-label">rows</div>
        </div>
      ))}
    </div>
  );
}

function ConfigPanel() {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Configuration</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="config-grid">
          <div className="config-item">
            <span className="config-label">Catalog</span>
            <Badge variant="secondary">{CONFIG.catalog}</Badge>
          </div>
          <div className="config-item">
            <span className="config-label">Schema</span>
            <Badge variant="secondary">{CONFIG.schema}</Badge>
          </div>
          <div className="config-item">
            <span className="config-label">Warehouse</span>
            <Badge variant="outline">{CONFIG.warehouseId}</Badge>
          </div>
          <div className="config-item">
            <span className="config-label">Lookback</span>
            <Badge variant="outline">{CONFIG.lookbackDays} days</Badge>
          </div>
          <div className="config-item">
            <span className="config-label">Confidence</span>
            <Badge variant="outline">{CONFIG.confidenceThreshold}</Badge>
          </div>
          <div className="config-item">
            <span className="config-label">Tables</span>
            <div className="config-tables">
              {CONFIG.tables.map((t) => (
                <Badge key={t} variant="secondary">
                  {t}
                </Badge>
              ))}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function GenieSpacePanel() {
  const [status, setStatus] = useState<"idle" | "running" | "done" | "error">("idle");
  const [genieUrl, setGenieUrl] = useState<string | null>(null);
  const [step, setStep] = useState(0);

  const steps = [
    "Extracting table metadata...",
    "Profiling column statistics...",
    "Mining query history...",
    "Generating sample queries via LLM...",
    "Assembling knowledge store...",
    "Validating payload...",
    "Creating Genie Space...",
  ];

  const handleCreate = () => {
    setStatus("running");
    setStep(0);
    setGenieUrl(null);

    const interval = setInterval(() => {
      setStep((prev) => {
        if (prev >= steps.length - 1) {
          clearInterval(interval);
          setStatus("done");
          setGenieUrl(
            `https://e2-demo-field-eng.cloud.databricks.com/genie/rooms/auto-genie-space`,
          );
          return prev;
        }
        return prev + 1;
      });
    }, 1500);
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Genie Space Deployment</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="genie-description">
          Create an AI-powered Genie Space from the configured catalog, schema, and tables.
          The pipeline will analyze table metadata, mine query history, generate sample queries,
          and deploy a fully configured Genie Space.
        </p>

        <div className="genie-config-summary">
          <div className="genie-config-row">
            <span>Target:</span>
            <strong>{CONFIG.catalog}.{CONFIG.schema}</strong>
          </div>
          <div className="genie-config-row">
            <span>Tables:</span>
            <strong>{CONFIG.tables.length} tables</strong>
          </div>
          <div className="genie-config-row">
            <span>Warehouse:</span>
            <strong>{CONFIG.warehouseId}</strong>
          </div>
        </div>

        {status === "idle" && (
          <button className="genie-create-btn" onClick={handleCreate}>
            Create Genie Space
          </button>
        )}

        {status === "running" && (
          <div className="genie-progress">
            {steps.map((s, i) => (
              <div
                key={i}
                className={`genie-step ${i < step ? "done" : i === step ? "active" : ""}`}
              >
                <span className="genie-step-indicator">
                  {i < step ? "\u2713" : i === step ? "\u25CF" : "\u25CB"}
                </span>
                <span>{s}</span>
              </div>
            ))}
          </div>
        )}

        {status === "done" && genieUrl && (
          <div className="genie-success">
            <div className="genie-success-icon">\u2713</div>
            <div className="genie-success-text">
              <strong>Genie Space Created Successfully!</strong>
              <p>Your AI-powered analytics space is ready for natural language queries.</p>
            </div>
            <a
              href={genieUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="genie-open-btn"
            >
              Open Genie Space
            </a>
          </div>
        )}

        {status === "error" && (
          <Alert variant="destructive">
            Failed to create Genie Space. Please check the configuration and try again.
          </Alert>
        )}
      </CardContent>
    </Card>
  );
}

export default function App() {
  const emptyParams = useMemo(() => ({}), []);

  return (
    <div className="app-container">
      <header className="app-header">
        <div className="header-content">
          <div className="header-left">
            <h1 className="app-title">AutoGenie</h1>
            <span className="app-subtitle">Sales Pipeline Analytics & Genie Space Builder</span>
          </div>
          <div className="header-right">
            <Badge variant="outline">{CONFIG.catalog}.{CONFIG.schema}</Badge>
          </div>
        </div>
      </header>

      <main className="app-main">
        <KpiSection />

        <section className="section-row">
          <TableOverview />
        </section>

        <Tabs defaultValue="pipeline">
          <TabsList>
            <TabsTrigger value="pipeline">Pipeline Analysis</TabsTrigger>
            <TabsTrigger value="accounts">Account Insights</TabsTrigger>
            <TabsTrigger value="reps">Rep Performance</TabsTrigger>
            <TabsTrigger value="genie">Genie Space</TabsTrigger>
          </TabsList>

          <TabsContent value="pipeline">
            <div className="charts-grid">
              <Card>
                <CardHeader>
                  <CardTitle>Pipeline by Stage</CardTitle>
                </CardHeader>
                <CardContent>
                  <BarChart
                    queryKey="pipeline_by_stage"
                    parameters={emptyParams}
                    xKey="stagename"
                    yKey="total_amount"
                    height={350}
                    colors={BRAND_COLORS}
                    title=""
                  />
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Monthly Opportunity Trend</CardTitle>
                </CardHeader>
                <CardContent>
                  <LineChart
                    queryKey="opportunity_trend"
                    parameters={emptyParams}
                    xKey="month"
                    yKey={["num_opportunities", "total_amount"]}
                    height={350}
                    colors={[BRAND_COLORS[0], BRAND_COLORS[1]]}
                    smooth
                    showLegend
                    title=""
                  />
                </CardContent>
              </Card>
            </div>

            <Card className="mt-6">
              <CardHeader>
                <CardTitle>Historical Pipeline by Deal Stage</CardTitle>
              </CardHeader>
              <CardContent>
                <BarChart
                  queryKey="pipeline_history"
                  parameters={emptyParams}
                  xKey="dealstage"
                  yKey="total_deal_amount"
                  height={300}
                  colors={["#4462c9"]}
                  orientation="horizontal"
                  title=""
                />
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="accounts">
            <div className="charts-grid">
              <Card>
                <CardHeader>
                  <CardTitle>Accounts by Industry</CardTitle>
                </CardHeader>
                <CardContent>
                  <DonutChart
                    queryKey="accounts_by_industry"
                    parameters={emptyParams}
                    xKey="industry"
                    yKey="account_count"
                    height={400}
                    colors={BRAND_COLORS}
                    showLegend
                    title=""
                  />
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Revenue by Region</CardTitle>
                </CardHeader>
                <CardContent>
                  <BarChart
                    queryKey="accounts_by_region"
                    parameters={emptyParams}
                    xKey="region"
                    yKey="total_revenue"
                    height={400}
                    colors={[BRAND_COLORS[1]]}
                    title=""
                  />
                </CardContent>
              </Card>
            </div>

            <Card className="mt-6">
              <CardHeader>
                <CardTitle>Account Details by Industry</CardTitle>
              </CardHeader>
              <CardContent>
                <DataTable queryKey="accounts_by_industry" parameters={emptyParams} />
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="reps">
            <div className="charts-grid">
              <Card>
                <CardHeader>
                  <CardTitle>Top 10 Sales Reps by Revenue</CardTitle>
                </CardHeader>
                <CardContent>
                  <BarChart
                    queryKey="top_reps"
                    parameters={emptyParams}
                    xKey="rep_name"
                    yKey="total_amount"
                    height={400}
                    colors={[BRAND_COLORS[0]]}
                    orientation="horizontal"
                    title=""
                  />
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Rep Deal Count vs Avg Deal Size</CardTitle>
                </CardHeader>
                <CardContent>
                  <BarChart
                    queryKey="top_reps"
                    parameters={emptyParams}
                    xKey="rep_name"
                    yKey={["deal_count", "avg_deal_size"]}
                    height={400}
                    colors={[BRAND_COLORS[0], BRAND_COLORS[2]]}
                    showLegend
                    title=""
                  />
                </CardContent>
              </Card>
            </div>

            <Card className="mt-6">
              <CardHeader>
                <CardTitle>Sales Rep Leaderboard</CardTitle>
              </CardHeader>
              <CardContent>
                <DataTable queryKey="top_reps" parameters={emptyParams} />
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="genie">
            <div className="genie-layout">
              <ConfigPanel />
              <GenieSpacePanel />
            </div>
          </TabsContent>
        </Tabs>
      </main>
    </div>
  );
}
