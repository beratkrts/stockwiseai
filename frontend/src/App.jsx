import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { AgGridReact } from "ag-grid-react";
import "ag-grid-community/styles/ag-grid.css";
import "ag-grid-community/styles/ag-theme-alpine.css";
import "./app.css";
import logo from "./assets/logo.png";

const API_BASE = import.meta.env.VITE_API_BASE || "/api";
const AUTO_REFRESH_MS = Number(import.meta.env.VITE_REFRESH_MS || 300000);
const CATEGORIES = [
  { label: "KUMAŞ", api: "KUMAŞ" },
  { label: "SLAT", api: "SLAT" },
  { label: "PROFIL", api: "Profil" },
  { label: "OTHER", api: "OTHER" }, // KUMAS/SLAT/Profil disindakiler
];

const INFO = {
  forecastStart: (
    <>
      <div className="info-section-title">Ne ifade eder?</div>
      <p>
        Her hafta, ilgili haftanin pazartesi gunu baz alinarak onumuzdeki 12 hafta icin tuketim tahmini uretilir.
      </p>
      <p>
        Bu tabloda gorulen tahmin degerleri, burada belirtilen tarihten sonra beklenen toplam tuketimi ifade eder.
      </p>
    </>
  ),
  materialList: (
    <>
      <div className="info-section-title">Ne ifade eder?</div>
      <p>Bu tablo, sistemdeki tum malzemeler icin stok (stock), tahmin (forecast) ve tahmin guvenilirligi (error rate) ozetini gosterir.</p>
      <div className="info-section-title">Stok</div>
      <p>
        Kumas malzemelerde, secilen rengin tum olcu varyantlarinin ana depodaki stoklari metrekare (m2) cinsinden
        toplanarak gosterilir.
      </p>
      <div className="info-section-title">Hata Orani</div>
      <p>
        Son 1 yil icerisindeki forecast sonuclarinin, gerceklesen tuketimle karsilastirilmasi sonucu hesaplanir. Oran ne
        kadar dusukse, mevcut tahmin o kadar guvenilirdir.
      </p>
    </>
  ),
  detailTable: (
    <>
      <div className="info-section-title">Ne ifade eder?</div>
      <p>Malzeme listesinden secilen malzemenin (material):</p>
      <ul>
        <li>Ayni renk ve item numarasina sahip tum varyantlarini (variants)</li>
        <li>Her varyantin depo bazli stok miktarini (warehouse stock)</li>
        <li>Son koltuk depo sayiminda tespit edilen miktari (seat count)</li>
        <li>Ve yolda olan (acik siparis) miktarini (in-transit order)</li>
      </ul>
      <p>detayli olarak gosterir.</p>
    </>
  ),
  flowView: (
    <>
      <div className="info-section-title">Ne ifade eder?</div>
      <p>Secilen malzemenin her bir varyanti icin, son koltuk depo sayimindan itibaren stok hareketlerini analiz eder.</p>
      <p>Bu tabloda:</p>
      <ul>
        <li>Son koltuk depo sayim tarihi (count date)</li>
        <li>Bu tarihten sonra BOM'a gore tahmini tuketim (seat consumption)</li>
        <li>Ayni donem icin ana depodan yapilan cikislar (main warehouse outflow)</li>
      </ul>
      <p>gosterilir.</p>
      <p>Bu bilgiler kullanilarak kullanicilar:</p>
      <div className="info-formula">Son sayim miktari - tahmini tuketim + ana depo cikislari</div>
      <p>formuluyle koltuk depodaki mevcut durumu yaklasik olarak degerlendirebilir.</p>
    </>
  ),
  openOrders: (
    <>
      <div className="info-section-title">Ne ifade eder?</div>
      <p>Acik satin alma siparisi bulunan tum malzemeler icin siparis durumunu gosterir.</p>
      <p>Tabloda:</p>
      <ul>
        <li>PO numarasi (PO)</li>
        <li>Siparis miktari (open qty)</li>
        <li>Tedarikci (company)</li>
        <li>Depoya giris yapildiysa giris miktari ve depo giris hareket ID'si (receipt qty, receipt H_ID)</li>
      </ul>
      <p>yer alir.</p>
      <div className="info-section-title">Kalan / Yolda Miktar (remaining qty)</div>
      <div className="info-formula">Siparis miktari - depoya giren miktar</div>
      <p>seklinde hesaplanir ve dashboard'daki "Yolda Siparis" bilgisi bu veriden uretilir.</p>
    </>
  ),
  overallMetrics: (
    <>
      <div className="info-section-title">Ne ifade eder?</div>
      <p>
        Bu tablo, sistemdeki tum malzemeler icin yapilan forecast sonuclarinin genel performans ozetini gosterir. Her
        malzeme icin secilen en iyi model (BEST) baz alinmistir.
      </p>
      <ul>
        <li>
          <strong>Scope:</strong> ALL_MATERIALS_BEST, her malzeme icin en dusuk hatayi veren modelin kullanildigini ifade
          eder.
        </li>
        <li>
          <strong>WAPE (weighted absolute percentage error):</strong> Tum malzemelerdeki toplam mutlak hatanin, toplam gerceklesen tuketime oranidir.
        </li>
        <li>
          <strong>MAE (mean absolute error):</strong> Ortalama mutlak hata degeridir. Bir tahminin ortalama ne kadar sapma yaptigini gosterir.
        </li>
        <li>
          <strong>Actual Sum (actual total):</strong> Backtest doneminde gerceklesen toplam tuketim miktaridir.
        </li>
        <li>
          <strong>N (sample size):</strong> Backtest hesaplamasinda kullanilan toplam gozlem (hafta x malzeme) sayisidir.
        </li>
      </ul>
    </>
  ),
  categoryUnitMetrics: (
    <>
      <div className="info-section-title">Ne ifade eder?</div>
      <p>
        Forecast performansini kategori ve olcu birimi bazinda kirilimli olarak gosterir. Bu sayede hangi malzeme
        gruplarinda tahminlerin daha guvenilir oldugu gorulebilir.
      </p>
      <ul>
        <li>
          <strong>Kategori (category):</strong> Malzemenin ana sinifini ifade eder (orn. KUMAS, SLAT, PROFIL).
        </li>
        <li>
          <strong>UOM (unit):</strong> Tahminlerin hangi birimde yapildigini gosterir (orn. m2, mt, adet).
        </li>
        <li>
          <strong>WAPE (weighted absolute percentage error):</strong> Ilgili kategori + birim icin hesaplanan agirlikli yuzde hata oranidir.
        </li>
        <li>
          <strong>MAE (mean absolute error):</strong> Bu grup icin ortalama mutlak hata degeridir.
        </li>
        <li>
          <strong>Actual Sum (actual total):</strong> Backtest donemindeki toplam gerceklesen tuketim miktaridir.
        </li>
        <li>
          <strong>N (sample size):</strong> Bu kategori ve birim icin kullanilan toplam gozlem sayisidir.
        </li>
      </ul>
    </>
  ),
  categoryFilters: (
    <>
      <div className="info-section-title">Ne ifade eder?</div>
      <p>
        Bu butonlar, metrikleri sadece secilen kategoriye ait malzemeler icin filtreler. Overall metrikler degismez,
        yalnizca kategori-birim detaylari guncellenir.
      </p>
    </>
  ),
  forecast12w: (
    <>
      <div className="info-section-title">Ne ifade eder?</div>
      <p>
        Secilen modele gore forecast start tarihinden itibaren 12 hafta icin beklenen toplam tuketimdir (12W forecast). Stok risk ve durum
        hesaplarinda kullanilir.
      </p>
    </>
  ),
};

const InfoTip = ({ title, children, align = "right" }) => {
  const [open, setOpen] = useState(false);
  const [popStyle, setPopStyle] = useState(null);
  const wrapperRef = useRef(null);
  const buttonRef = useRef(null);
  const popoverRef = useRef(null);

  useEffect(() => {
    if (!open) return undefined;
    const handleClick = (event) => {
      const inWrapper = wrapperRef.current && wrapperRef.current.contains(event.target);
      const inPopover = popoverRef.current && popoverRef.current.contains(event.target);
      if (!inWrapper && !inPopover) {
        setOpen(false);
      }
    };
    const handleKey = (event) => {
      if (event.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", handleClick);
    document.addEventListener("keydown", handleKey);
    return () => {
      document.removeEventListener("mousedown", handleClick);
      document.removeEventListener("keydown", handleKey);
    };
  }, [open]);

  useEffect(() => {
    if (!open) return undefined;
    const updatePosition = () => {
      if (!buttonRef.current) return;
      const rect = buttonRef.current.getBoundingClientRect();
      const gap = 8;
      const maxWidth = 320;
      const viewportWidth = window.innerWidth || maxWidth;
      const width = Math.min(maxWidth, viewportWidth - gap * 2);
      let left = align === "left" ? rect.left : rect.right - width;
      left = Math.min(Math.max(gap, left), viewportWidth - gap - width);
      const top = rect.bottom + gap;
      setPopStyle({
        top: `${top}px`,
        left: `${left}px`,
        width: `${width}px`,
      });
    };
    updatePosition();
    window.addEventListener("scroll", updatePosition, true);
    window.addEventListener("resize", updatePosition);
    return () => {
      window.removeEventListener("scroll", updatePosition, true);
      window.removeEventListener("resize", updatePosition);
    };
  }, [open, align]);

  return (
    <div ref={wrapperRef} className={`info-tip ${align === "left" ? "left" : "right"}`}>
      <button
        type="button"
        className="info-btn"
        aria-label={`Info: ${title}`}
        ref={buttonRef}
        onClick={(event) => {
          event.stopPropagation();
          setOpen((prev) => !prev);
        }}
      >
        <span aria-hidden="true">i</span>
      </button>
      {open &&
        createPortal(
          <div
            ref={popoverRef}
            className="info-pop"
            role="dialog"
            aria-label={title}
            style={popStyle || { visibility: "hidden" }}
          >
            <div className="info-title">{title}</div>
            <div className="info-body">{children}</div>
          </div>,
          document.body
        )}
    </div>
  );
};

const HeaderWithInfo = (props) => {
  const { displayName, tooltipTitle, tooltipContent } = props;
  const handleSortClick = (event) => {
    if (props.progressSort) {
      props.progressSort(event.shiftKey);
      return;
    }
    if (props.setSort) {
      if (!props.sort) {
        props.setSort("asc", event.shiftKey);
      } else if (props.sort === "asc") {
        props.setSort("desc", event.shiftKey);
      } else {
        props.setSort(null, event.shiftKey);
      }
    }
  };
  return (
    <div className="ag-header-with-info">
      <button type="button" className="ag-header-sort-btn" onClick={handleSortClick}>
        {displayName}
      </button>
      <InfoTip title={tooltipTitle}>{tooltipContent}</InfoTip>
    </div>
  );
};

const StatusCell = (params) => {
  const s = (params.value || "").toUpperCase();
  const base = "badge";
  if (s === "CRITICAL") return <span className={`${base} critical`}>Critical</span>;
  if (s === "MEDIUM") return <span className={`${base} medium`}>Medium</span>;
  if (s === "SAFE") return <span className={`${base} safe`}>Safe</span>;
  if (s === "EN_BILGISI_EKSIK") return <span className={`${base} en-bilgisi-eksik`}>Missing width info</span>;
  return <span className={base}>{params.value || ""}</span>;
};

function App() {
  const [view, setView] = useState("dashboard");
  const [category, setCategory] = useState(CATEGORIES[0]);
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [supplierFilter, setSupplierFilter] = useState("");
  const [itemNoFilter, setItemNoFilter] = useState("");
  const [pageSize, setPageSize] = useState(50);
  const [total, setTotal] = useState(0);
  const [selected, setSelected] = useState(null);
  const [variants, setVariants] = useState([]);
  const [flowRows, setFlowRows] = useState([]);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [loadingFlow, setLoadingFlow] = useState(false);
  const [supplierOptions, setSupplierOptions] = useState([]);
  const [materialSuppliers, setMaterialSuppliers] = useState([]);
  const [loadingSuppliers, setLoadingSuppliers] = useState(false);
  const [forecastCategory, setForecastCategory] = useState(CATEGORIES[0]);
  const [forecastPageSize, setForecastPageSize] = useState(50);
  const [forecastPage, setForecastPage] = useState(1);
  const [forecastRows, setForecastRows] = useState([]);
  const [forecastOverall, setForecastOverall] = useState([]);
  const [forecastTotal, setForecastTotal] = useState(0);
  const [loadingForecast, setLoadingForecast] = useState(false);
  const [firstForecastDate, setFirstForecastDate] = useState(null);
  const [lastForecastDate, setLastForecastDate] = useState(null);
  const [openOrders, setOpenOrders] = useState([]);
  const [openOrdersTotal, setOpenOrdersTotal] = useState(0);
  const [openOrdersPageSize, setOpenOrdersPageSize] = useState(50);
  const [openOrdersPage, setOpenOrdersPage] = useState(1);
  const [openOrdersSearch, setOpenOrdersSearch] = useState("");
  const [openOrdersHidSearch, setOpenOrdersHidSearch] = useState("");
  const [loadingOpenOrders, setLoadingOpenOrders] = useState(false);
  const [openOrdersCategory, setOpenOrdersCategory] = useState("");
  const [openOrdersRemainingFilter, setOpenOrdersRemainingFilter] = useState("all");
  const [openOrdersWithReceipts, setOpenOrdersWithReceipts] = useState(false);
  const [isOverviewOpen, setIsOverviewOpen] = useState(true);
  const [isDetailOpen, setIsDetailOpen] = useState(true);
  const [isSuppliersOpen, setIsSuppliersOpen] = useState(true);
  const [isFlowOpen, setIsFlowOpen] = useState(true);
  const [isGridReady, setIsGridReady] = useState(false);

  const formatWape = (value) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
    return `${Number(value).toFixed(2)}%`;
  };

  const formatNumber = (value) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
    return Number(value).toFixed(2);
  };

  useEffect(() => {
    let timerId = null;
    const fetchForecastMeta = async () => {
      const res = await fetch(`${API_BASE}/forecast-meta`);
      const data = await res.json();
      setFirstForecastDate(data.first_forecast_date || null);
      setLastForecastDate(data.last_forecast_date || null);
    };
    fetchForecastMeta().catch(console.error);
    if (Number.isFinite(AUTO_REFRESH_MS) && AUTO_REFRESH_MS > 0) {
      timerId = window.setInterval(() => {
        fetchForecastMeta().catch(console.error);
      }, AUTO_REFRESH_MS);
    }
    return () => {
      if (timerId) window.clearInterval(timerId);
    };
  }, []);

  const gridApiRef = useRef(null);

  const dataSource = useMemo(
    () => ({
      getRows: async (params) => {
        const apiCategory = category?.api || "KUMAŞ";
        const activeSort = params.sortModel?.[0] || {};
        const sortBy = activeSort.colId || "";
        const sortDir = activeSort.sort || "";
        const page = Math.floor(params.startRow / pageSize) + 1;
        const url = `${API_BASE}/materials?category=${encodeURIComponent(apiCategory)}&page=${page}&page_size=${pageSize}&q=${encodeURIComponent(search)}&status=${encodeURIComponent(statusFilter || "")}&supplier=${encodeURIComponent(supplierFilter || "")}&item_no=${encodeURIComponent(itemNoFilter || "")}&sort_by=${encodeURIComponent(sortBy)}&sort_dir=${encodeURIComponent(sortDir)}`;
        try {
          const res = await fetch(url);
          const data = await res.json();
          const items = data.items || [];
          const totalCount = data.total ?? 0;
          setTotal(totalCount);
          if (params.startRow === 0) {
            if (items.length) {
              const first = items[0].bom_material_name;
              setSelected((prev) => (prev && items.some((i) => i.bom_material_name === prev) ? prev : first));
            } else {
              setSelected(null);
              setVariants([]);
              setFlowRows([]);
            }
          }
          params.successCallback(items, totalCount);
        } catch (err) {
          console.error(err);
          params.failCallback();
        }
      },
    }),
    [category, search, statusFilter, supplierFilter, itemNoFilter, pageSize]
  );

  useEffect(() => {
    if (view !== "dashboard") {
      return;
    }
    const api = gridApiRef.current;
    if (!api) return;
    api.setGridOption("datasource", dataSource);
    api.purgeInfiniteCache();
  }, [view, dataSource]);

  useEffect(() => {
    if (view !== "dashboard" || !isGridReady) {
      return;
    }
    if (!Number.isFinite(AUTO_REFRESH_MS) || AUTO_REFRESH_MS <= 0) {
      return;
    }
    const timerId = window.setInterval(() => {
      gridApiRef.current?.refreshInfiniteCache();
    }, AUTO_REFRESH_MS);
    return () => window.clearInterval(timerId);
  }, [view, isGridReady]);

  useEffect(() => {
    if (view !== "dashboard") {
      return;
    }
    const fetchSuppliers = async () => {
      const res = await fetch(`${API_BASE}/suppliers`);
      const data = await res.json();
      setSupplierOptions(data.items || []);
    };
    fetchSuppliers().catch(console.error);
  }, [view]);

  useEffect(() => {
    if (view !== "dashboard") {
      return;
    }
    let timerId = null;
    const fetchDetail = async () => {
      if (!selected) {
        setVariants([]);
        setFlowRows([]);
        return;
      }
      setLoadingDetail(true);
      setLoadingFlow(true);
      try {
        const variantPromise = fetch(`${API_BASE}/materials/${encodeURIComponent(selected)}/variants`).then((res) => res.json());

        fetch(`${API_BASE}/materials/${encodeURIComponent(selected)}/flow-observation`)
          .then((res) => res.json())
          .then((flowData) => setFlowRows(flowData.items || []))
          .finally(() => setLoadingFlow(false));

        const variantData = await variantPromise;
        setVariants(variantData.items || []);
      } finally {
        setLoadingDetail(false);
      }
    };
    fetchDetail().catch(console.error);
    if (Number.isFinite(AUTO_REFRESH_MS) && AUTO_REFRESH_MS > 0) {
      timerId = window.setInterval(() => {
        fetchDetail().catch(console.error);
      }, AUTO_REFRESH_MS);
    }
    return () => {
      if (timerId) window.clearInterval(timerId);
    };
  }, [view, selected]);

  useEffect(() => {
    if (view !== "dashboard") {
      return;
    }
    if (!selected) {
      setMaterialSuppliers([]);
      return;
    }
    setLoadingSuppliers(true);
    fetch(`${API_BASE}/materials/${encodeURIComponent(selected)}/suppliers`)
      .then((res) => res.json())
      .then((data) => setMaterialSuppliers(data.items || []))
      .catch(console.error)
      .finally(() => setLoadingSuppliers(false));
  }, [view, selected]);

  useEffect(() => {
    if (view !== "forecast") {
      return;
    }
    let timerId = null;
    const fetchForecast = async () => {
      setLoadingForecast(true);
      try {
        const apiCategory = forecastCategory?.api || "KUMAŞ";
        const url = `${API_BASE}/forecast-details?category=${encodeURIComponent(apiCategory)}&page=${forecastPage}&page_size=${forecastPageSize}`;
        const res = await fetch(url);
        const data = await res.json();
        setForecastRows(data.category_unit || []);
        setForecastOverall(data.overall || []);
        setForecastTotal(data.total || 0);
      } finally {
        setLoadingForecast(false);
      }
    };
    fetchForecast().catch(console.error);
    if (Number.isFinite(AUTO_REFRESH_MS) && AUTO_REFRESH_MS > 0) {
      timerId = window.setInterval(() => {
        fetchForecast().catch(console.error);
      }, AUTO_REFRESH_MS);
    }
    return () => {
      if (timerId) window.clearInterval(timerId);
    };
  }, [view, forecastCategory, forecastPage, forecastPageSize]);

  useEffect(() => {
    if (view !== "open-orders") {
      return;
    }
    let timerId = null;
    const fetchOpenOrders = async () => {
      setLoadingOpenOrders(true);
      try {
        const url = `${API_BASE}/open-orders?page=${openOrdersPage}&page_size=${openOrdersPageSize}&q=${encodeURIComponent(openOrdersSearch)}&h_id=${encodeURIComponent(openOrdersHidSearch)}`;
        const res = await fetch(url);
        const data = await res.json();
        setOpenOrders(data.items || []);
        setOpenOrdersTotal(data.total || 0);
      } finally {
        setLoadingOpenOrders(false);
      }
    };
    fetchOpenOrders().catch(console.error);
    if (Number.isFinite(AUTO_REFRESH_MS) && AUTO_REFRESH_MS > 0) {
      timerId = window.setInterval(() => {
        fetchOpenOrders().catch(console.error);
      }, AUTO_REFRESH_MS);
    }
    return () => {
      if (timerId) window.clearInterval(timerId);
    };
  }, [view, openOrdersPage, openOrdersPageSize, openOrdersSearch, openOrdersHidSearch]);

  const columnDefs = useMemo(
    () => [
      { headerName: "Material", field: "bom_material_name", sortable: true, filter: true, flex: 1, minWidth: 220 },
      { headerName: "Item No", field: "item_no", width: 110 },
      { headerName: "Unit", field: "unit_of_measure", width: 110 },
      { headerName: "Category", field: "material_category", width: 110 },
      {
        headerName: "12W Forecast",
        field: "forecast_12w",
        width: 120,
        headerComponent: HeaderWithInfo,
        headerComponentParams: {
          tooltipTitle: "12W Forecast",
          tooltipContent: INFO.forecast12w,
        },
      },
      { headerName: "Error Rate (1Y)", field: "wape", width: 110, valueFormatter: (p) => formatWape(p.value) },
      { headerName: "Stock", field: "current_stock", width: 100 },
      {
        headerName: "Status",
        field: "safety_status",
        width: 110,
        cellRenderer: StatusCell,
      },
    ],
    []
  );

  const rowClassRules = {
    "status-row-critical": (params) => params.data && params.data.safety_status === "CRITICAL",
    "status-row-medium": (params) => params.data && params.data.safety_status === "MEDIUM",
    "status-row-safe": (params) => params.data && params.data.safety_status === "SAFE",
    "status-row-en-bilgisi-eksik": (params) => params.data && params.data.safety_status === "EN_BILGISI_EKSIK",
  };

  const handleExportMaterialsExcel = async () => {
    const api = gridApiRef.current;
    if (!api) return;
    const sortModel = api.getSortModel?.() || [];
    const activeSort = sortModel[0] || {};
    const params = new URLSearchParams();
    params.set("category", category?.api || "KUMAŞ");
    if (search) params.set("q", search);
    if (statusFilter) params.set("status", statusFilter);
    if (supplierFilter) params.set("supplier", supplierFilter);
    if (itemNoFilter) params.set("item_no", itemNoFilter);
    if (activeSort.colId) params.set("sort_by", activeSort.colId);
    if (activeSort.sort) params.set("sort_dir", activeSort.sort);
    const res = await fetch(`${API_BASE}/materials-export?${params.toString()}`);
    if (!res.ok) {
      console.error("Export failed", res.status);
      return;
    }
    const blob = await res.blob();
    const url = window.URL.createObjectURL(blob);
    const contentDisposition = res.headers.get("content-disposition") || "";
    const match = contentDisposition.match(/filename="([^"]+)"/i);
    const filename = match ? match[1] : "material_list.xlsx";
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  };

  const forecastTotalPages = Math.max(Math.ceil(forecastTotal / forecastPageSize), 1);
  const openOrdersTotalPages = Math.max(Math.ceil(openOrdersTotal / openOrdersPageSize), 1);
  const seatInfo = flowRows[0] || {};
  const openOrdersCategories = useMemo(() => {
    const names = new Set();
    openOrders.forEach((row) => {
      if (row.material_category) {
        names.add(row.material_category);
      }
    });
    return Array.from(names).sort();
  }, [openOrders]);
  const filteredOpenOrders = useMemo(() => {
    return openOrders.filter((row) => {
      if (openOrdersCategory && row.material_category !== openOrdersCategory) {
        return false;
      }
      const remaining = Number(row.remaining_qty);
      if (openOrdersRemainingFilter === "positive" && !(remaining > 0)) return false;
      if (openOrdersRemainingFilter === "zero" && remaining !== 0) return false;
      if (openOrdersRemainingFilter === "negative" && !(remaining < 0)) return false;
      if (openOrdersWithReceipts && (!row.received_hids || row.received_hids.length === 0)) return false;
      return true;
    });
  }, [openOrders, openOrdersCategory, openOrdersRemainingFilter, openOrdersWithReceipts]);

  return (
    <>
      <header className="header">
        <div className="brand-text">
          <img className="brand-logo" src={logo} alt="TKIS StockWiseAI" />
        </div>
        <div className="header-right">
          <div className="header-meta">
            <span>Forecast start: {firstForecastDate || "-"}</span>
            <InfoTip title="Forecast Baslangic Tarihi" align="left">
              {INFO.forecastStart}
            </InfoTip>
          </div>
          <div className="page-tabs">
            <button className={`tab ${view === "dashboard" ? "active" : ""}`} onClick={() => setView("dashboard")}>
              Dashboard
            </button>
            <button className={`tab ${view === "forecast" ? "active" : ""}`} onClick={() => setView("forecast")}>
              Forecast Details
            </button>
            <button className={`tab ${view === "open-orders" ? "active" : ""}`} onClick={() => setView("open-orders")}>
              Open Orders
            </button>
          </div>
          <div className="badge safe">React + AG Grid</div>
        </div>
      </header>

      <div className="panel">
        {view === "dashboard" ? (
          <>
            <div className="tabs">
              {CATEGORIES.map((cat) => (
                <button key={cat.label} className={`tab ${cat.label === category.label ? "active" : ""}`} onClick={() => { setCategory(cat); setPage(1); }}>
                  {cat.label}
                </button>
              ))}
              <span className="tabs-spacer" />
              <InfoTip title="Kategori Filtreleri">
                {INFO.categoryFilters}
              </InfoTip>
            </div>

            <div className="filters">
              <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search material..." />
              <input value={itemNoFilter} onChange={(e) => setItemNoFilter(e.target.value)} placeholder="Search Item No..." />
              <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
                <option value="">Status (All)</option>
                <option value="CRITICAL">CRITICAL</option>
                <option value="MEDIUM">MEDIUM</option>
                <option value="SAFE">SAFE</option>
                <option value="EN_BILGISI_EKSIK">MISSING WIDTH INFO</option>
              </select>
              <input
                list="supplier-options"
                value={supplierFilter}
                onChange={(e) => setSupplierFilter(e.target.value)}
                placeholder="Search supplier..."
              />
              <datalist id="supplier-options">
                {supplierOptions.map((supplier) => (
                  <option key={supplier} value={supplier} />
                ))}
              </datalist>
              <select value={pageSize} onChange={(e) => setPageSize(Number(e.target.value))}>
                {[25, 50, 100, 200].map((n) => (
                  <option key={n} value={n}>
                    {n} / page
                  </option>
                ))}
              </select>
              <span>Total: {total}</span>
            </div>

            <div className="dashboard-grid">
              <div className="card resizable-card card-large">
                <div className="card-header">
                  <h2>Material List</h2>
                  <div className="card-header-actions">
                    <InfoTip title="Malzeme Listesi">
                      {INFO.materialList}
                    </InfoTip>
                    <button className="toggle-btn" onClick={handleExportMaterialsExcel}>
                      Export Excel
                    </button>
                    <button className="toggle-btn" onClick={() => setIsOverviewOpen((v) => !v)}>
                      {isOverviewOpen ? "Hide" : "Show"}
                    </button>
                  </div>
                </div>
                {isOverviewOpen && (
                  <div className="card-body">
                    <div className="ag-theme-alpine grid grid-large">
                      <AgGridReact
                        rowModelType="infinite"
                        cacheBlockSize={pageSize}
                        columnDefs={columnDefs}
                        onGridReady={(params) => {
                          gridApiRef.current = params.api;
                          params.api.setGridOption("datasource", dataSource);
                          setIsGridReady(true);
                        }}
                        onSortChanged={() => {
                          gridApiRef.current?.purgeInfiniteCache();
                        }}
                        onRowClicked={(e) => setSelected(e.data.bom_material_name)}
                        rowClassRules={rowClassRules}
                        animateRows
                        overlayNoRowsTemplate="<span>No results</span>"
                        overlayLoadingTemplate="<span>Loading...</span>"
                      />
                    </div>
                  </div>
                )}
              </div>

              <div className="dashboard-side">
                <div className="card resizable-card">
                  <div className="card-header">
                    <h2>Details</h2>
                    <div className="card-header-actions">
                      <InfoTip title="Detay Tablosu">
                        {INFO.detailTable}
                      </InfoTip>
                      <button className="toggle-btn" onClick={() => setIsDetailOpen((v) => !v)}>
                        {isDetailOpen ? "Hide" : "Show"}
                      </button>
                    </div>
                  </div>
                  {isDetailOpen && (
                    <div className="card-body">
                      <div className="selected">{selected || "No selection"}</div>
                      {loadingDetail ? (
                        <div>Loading...</div>
                      ) : (
                        <div className="detail-scroll detail-scroll-wide">
                          <table className="detail-table">
                            <thead>
                              <tr>
                            <th>Stock Name</th>
                            <th>Warehouse</th>
                            <th>Stock</th>
                            <th>In-transit Order</th>
                            <th>Unit</th>
                            <th>Item No</th>
                            <th>Supplier 1</th>
                            <th>Supplier 2</th>
                            <th>Supplier 3</th>
                            <th>Supplier 4</th>
                            <th>Supplier 5</th>
                          </tr>
                        </thead>
                        <tbody>
                          {variants.length === 0 ? (
                            <tr>
                              <td colSpan="11">No stock found</td>
                            </tr>
                          ) : (
                            variants.map((v, idx) => (
                              <tr key={idx}>
                                <td>{v.stock_adi}</td>
                                <td>{v.warehouse}</td>
                                <td>{v.current_stock}</td>
                                <td>{v.open_order_in_transit ?? "-"}</td>
                                <td>{v.stock_uom}</td>
                                <td>{v.item_no ?? "-"}</td>
                                <td>{v.tedarikci_1 || "-"}</td>
                                <td>{v.tedarikci_2 || "-"}</td>
                                <td>{v.tedarikci_3 || "-"}</td>
                                    <td>{v.tedarikci_4 || "-"}</td>
                                    <td>{v.tedarikci_5 || "-"}</td>
                                  </tr>
                                ))
                              )}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>
                  )}
                </div>

                <div className="card resizable-card">
                  <div className="card-header">
                    <h2>Suppliers</h2>
                    <button className="toggle-btn" onClick={() => setIsSuppliersOpen((v) => !v)}>
                      {isSuppliersOpen ? "Hide" : "Show"}
                    </button>
                  </div>
                  {isSuppliersOpen && (
                    <div className="card-body">
                      <div className="selected">{selected ? `Material: ${selected}` : "No selection"}</div>
                      {loadingSuppliers ? (
                        <div>Loading...</div>
                      ) : materialSuppliers.length === 0 ? (
                        <div>No records found</div>
                      ) : (
                        <div className="supplier-list">
                          {materialSuppliers.map((supplier) => (
                            <span key={supplier} className="supplier-chip">
                              {supplier}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                </div>

                <div className="card resizable-card">
                  <div className="card-header">
                    <h2>Material Flow View</h2>
                    <div className="card-header-actions">
                      <InfoTip title="Malzeme Akis Gorunumu">
                        {INFO.flowView}
                      </InfoTip>
                      <button className="toggle-btn" onClick={() => setIsFlowOpen((v) => !v)}>
                        {isFlowOpen ? "Hide" : "Show"}
                      </button>
                    </div>
                  </div>
                  {isFlowOpen && (
                    <div className="card-body">
                      <div className="selected">{selected ? `Filter: ${selected}` : "No selection"}</div>
                      {loadingFlow ? (
                        <div>Loading...</div>
                      ) : (
                        <>
                          <div className="flow-meta">
                            <span>
                              Seat consumption: <strong>{seatInfo.seat_consumed_qty ?? "-"}</strong>{" "}
                              {seatInfo.seat_consumed_uom || ""}
                            </span>
                            <span>Count date: {seatInfo.count_end_date || "-"}</span>
                          </div>
                          <div className="detail-scroll">
                            <table className="detail-table">
                              <thead>
                                <tr>
                                  <th>Warehouse</th>
                                  <th>Stock Name</th>
                                  <th>Main Warehouse Outflow</th>
                                  <th>Main Warehouse Unit</th>
                                  <th>Count Date</th>
                                </tr>
                              </thead>
                              <tbody>
                                {flowRows.length === 0 ? (
                                  <tr>
                                    <td colSpan="5">No records found</td>
                                  </tr>
                                ) : (
                                  flowRows.map((row, idx) => (
                                    <tr key={idx}>
                                      <td>{row.warehouse}</td>
                                      <td>{row.stock_adi}</td>
                                      <td>{row.w22_out_qty}</td>
                                      <td>{row.w22_out_uom}</td>
                                      <td>{row.count_end_date}</td>
                                    </tr>
                                  ))
                                )}
                              </tbody>
                            </table>
                          </div>
                        </>
                      )}
                    </div>
                  )}
                </div>
              </div>
            </div>
          </>
        ) : view === "forecast" ? (
          <>
            <div className="card">
              <div className="card-header">
                <h2>Overall Metrics</h2>
                <InfoTip title="Overall Metrikler">
                  {INFO.overallMetrics}
                </InfoTip>
              </div>
              {loadingForecast ? (
                <div>Loading...</div>
              ) : (
                <div className="detail-scroll">
                  <table className="detail-table">
                    <thead>
                      <tr>
                        <th>Scope</th>
                        <th>WAPE</th>
                        <th>MAE</th>
                        <th>Actual Sum</th>
                        <th>N</th>
                      </tr>
                    </thead>
                    <tbody>
                      {forecastOverall.length === 0 ? (
                        <tr>
                          <td colSpan="5">No records found</td>
                        </tr>
                      ) : (
                        forecastOverall.map((row, idx) => (
                          <tr key={idx}>
                            <td>{row.scope}</td>
                            <td>{formatWape(row.wape)}</td>
                            <td>{formatNumber(row.mae)}</td>
                            <td>{formatNumber(row.actual_sum)}</td>
                            <td>{row.n_points ?? "-"}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            <div className="tabs">
              {CATEGORIES.map((cat) => (
                <button key={cat.label} className={`tab ${cat.label === forecastCategory.label ? "active" : ""}`} onClick={() => { setForecastCategory(cat); setForecastPage(1); }}>
                  {cat.label}
                </button>
              ))}
              <span className="tabs-spacer" />
              <InfoTip title="Kategori Filtreleri">
                {INFO.categoryFilters}
              </InfoTip>
            </div>

            <div className="filters">
              <select value={forecastPageSize} onChange={(e) => { setForecastPage(1); setForecastPageSize(Number(e.target.value)); }}>
                {[25, 50, 100, 200].map((n) => (
                  <option key={n} value={n}>
                    {n} / page
                  </option>
                ))}
              </select>
              <span>Total: {forecastTotal}</span>
            </div>

            <div className="card">
              <div className="card-header">
                <h2>Category - Unit Metrics</h2>
                <InfoTip title="Kategori - Birim Metrikler">
                  {INFO.categoryUnitMetrics}
                </InfoTip>
              </div>
              {loadingForecast ? (
                <div>Loading...</div>
              ) : (
                <div className="detail-scroll">
                  <table className="detail-table">
                    <thead>
                      <tr>
                        <th>Category</th>
                        <th>Unit</th>
                        <th>WAPE</th>
                        <th>MAE</th>
                        <th>Actual Sum</th>
                        <th>N</th>
                      </tr>
                    </thead>
                    <tbody>
                      {forecastRows.length === 0 ? (
                        <tr>
                          <td colSpan="6">No records found</td>
                        </tr>
                      ) : (
                        forecastRows.map((row, idx) => (
                          <tr key={idx}>
                            <td>{row.bom_material_category}</td>
                            <td>{row.bom_unit_of_measure}</td>
                            <td>{formatWape(row.wape)}</td>
                            <td>{formatNumber(row.mae)}</td>
                            <td>{formatNumber(row.actual_sum)}</td>
                            <td>{row.n_points ?? "-"}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              )}
              <div className="pager">
                <button disabled={forecastPage <= 1} onClick={() => setForecastPage((p) => Math.max(1, p - 1))}>
                  Previous
                </button>
                <span>
                  Page {forecastPage}/{forecastTotalPages}
                </span>
                <button disabled={forecastPage >= forecastTotalPages} onClick={() => setForecastPage((p) => Math.min(forecastTotalPages, p + 1))}>
                  Next
                </button>
              </div>
            </div>
          </>
        ) : (
          <>
            <div className="filters">
              <input value={openOrdersSearch} onChange={(e) => { setOpenOrdersPage(1); setOpenOrdersSearch(e.target.value); }} placeholder="Search material..." />
              <input value={openOrdersHidSearch} onChange={(e) => { setOpenOrdersPage(1); setOpenOrdersHidSearch(e.target.value); }} placeholder="Search PO..." />
              <select value={openOrdersCategory} onChange={(e) => setOpenOrdersCategory(e.target.value)}>
                <option value="">Category (All)</option>
                {openOrdersCategories.map((cat) => (
                  <option key={cat} value={cat}>
                    {cat}
                  </option>
                ))}
              </select>
              <select value={openOrdersRemainingFilter} onChange={(e) => setOpenOrdersRemainingFilter(e.target.value)}>
                <option value="all">Remaining (All)</option>
                <option value="positive">Remaining {">"} 0</option>
                <option value="zero">Remaining = 0</option>
                <option value="negative">Remaining {"<"} 0</option>
              </select>
              <label className="filter-check">
                <input type="checkbox" checked={openOrdersWithReceipts} onChange={(e) => setOpenOrdersWithReceipts(e.target.checked)} />
                Has receipt match
              </label>
              <select value={openOrdersPageSize} onChange={(e) => { setOpenOrdersPage(1); setOpenOrdersPageSize(Number(e.target.value)); }}>
                {[25, 50, 100, 200].map((n) => (
                  <option key={n} value={n}>
                    {n} / page
                  </option>
                ))}
              </select>
              <span>Total: {openOrdersTotal}</span>
            </div>

            <div className="card">
              <div className="card-header">
                <h2>Open Orders (Receipt Offsets)</h2>
                <InfoTip title="Acik Siparisler (Yolda / Kalan)">
                  {INFO.openOrders}
                </InfoTip>
              </div>
              {loadingOpenOrders ? (
                <div>Loading...</div>
              ) : (
                <div className="detail-scroll table-scroll-large">
                  <table className="detail-table open-orders-table">
                    <thead>
                      <tr>
                        <th>PO</th>
                        <th>Date</th>
                        <th>Material</th>
                        <th>Company</th>
                        <th>Category</th>
                        <th>Unit</th>
                        <th>Open Qty</th>
                        <th>Receipt Qty</th>
                        <th>Remaining Qty</th>
                        <th>Receipt H_ID</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredOpenOrders.length === 0 ? (
                        <tr>
                          <td colSpan="10">No records found</td>
                        </tr>
                      ) : (
                        filteredOpenOrders.map((row, idx) => (
                          <tr key={idx}>
                            <td>{row.h_id}</td>
                            <td>{row.transaction_date}</td>
                            <td>{row.material_name}</td>
                            <td>{row.company_code}</td>
                            <td>{row.material_category}</td>
                            <td>{row.unit_of_measure}</td>
                            <td>{formatNumber(row.open_qty)}</td>
                            <td>{formatNumber(row.received_qty)}</td>
                            <td>{formatNumber(row.remaining_qty)}</td>
                            <td>{Array.isArray(row.received_hids) ? row.received_hids.join(", ") : row.received_hids || "-"}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              )}
              <div className="pager">
                <button disabled={openOrdersPage <= 1} onClick={() => setOpenOrdersPage((p) => Math.max(1, p - 1))}>
                  Previous
                </button>
                <span>
                  Page {openOrdersPage}/{openOrdersTotalPages}
                </span>
                <button disabled={openOrdersPage >= openOrdersTotalPages} onClick={() => setOpenOrdersPage((p) => Math.min(openOrdersTotalPages, p + 1))}>
                  Next
                </button>
              </div>
            </div>
          </>
        )}
      </div>
    </>
  );
}

export default App;
