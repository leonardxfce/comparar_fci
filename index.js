// --- Constants and Configuration ---
const DATA_KEYS = {
  VARIATION_INTD: "Variac. %",
  VARIATION_YTD: "30/12/24",
  FUND_NAME: "Fondo_Fondo",
  COD: "Código de Clasificación_Código de Clasificación",
  MONEDA: "Moneda Fondo_Moneda Fondo",
};
const INFLACION = "Inflación";
const BENCHMARK_GARANTIZADO_DATA = [
  { ted: 0.08219, nombre: "Cuenta Remunerada Banco Bica 30% TNA 🏛" },
  { ted: 0.08493, nombre: "Cuenta Remunerada Naranja X 31% TNA 🏛" },
  { ted: 0.09589, nombre: "Cuenta Remunerada Uala 35% TNA 🏛" },
  { ted: 0.08219, nombre: "Cuenta Remunerada Uala Base 30% TNA 🏛" },
];
const ELEMENT_IDS = {
  FCI_DATA_CLASS: "fci_data",
  DATOS_FINANCIEROS: "datosFinancieros",
  BARCHAR: "myHorizontalBarChart",
};

const TOP_FUNDS_LIMIT = 10;

function callbackfn1(i) {
  if (i[DATA_KEYS.FUND_NAME].includes(INFLACION)) {
    return "red";
  }

  if (i[DATA_KEYS.FUND_NAME].includes("Cuenta Remunerada")) {
    return "rgb(70, 130, 180)";
  }

  switch (i[DATA_KEYS.COD]) {
    case 3:
      return "rgb(173, 216, 230)";
    case 2:
      return "rgb(60, 179, 113)";
    default:
      return "rgb(255, 165, 0)";
  }
}

function getOptions(text) {
  return {
    indexAxis: "y",
    aspectRatio: 1,
    scales: {
      x: {
        beginAtZero: true,
        title: {
          display: true,
          text: "Variación (%)",
        },
      },
      y: {
        title: {
          display: false,
          text: "Fondo",
        },
      },
    },
    plugins: {
      legend: {
        display: false,
      },
      title: {
        display: true,
        text: text,
      },
      tooltip: {
        callbacks: {
          label: function (context) {
            let label = context.dataset.label || "";
            if (label) {
              label += ": ";
            }
            if (context.parsed.x !== null) {
              label += context.parsed.x.toFixed(3) + "%";
            }
            return label;
          },
        },
      },
    },
  };
}

function getChartData(labels, dataValues, colors) {
  return {
    labels: labels,
    datasets: [
      {
        label: "Variación %",
        data: dataValues,
        backgroundColor: colors,
        borderColor: colors,
        borderWidth: 1,
      },
    ],
  };
}

function extracted(dataArray) {
  const x = dataArray.every((i) => i[DATA_KEYS.FUND_NAME].includes("Clase A"));
  const tipo = x ? "PARA PERSONAS" : "TODOS LOS FCI";
  return tipo;
}

function crearDashboardHorizontalBarras(dataArray, index, isYTd, tipo) {
  const callbackfn3 = (item) => item[DATA_KEYS.FUND_NAME];
  const labels = dataArray.map(callbackfn3);
  const callbackfn2 = (item) =>
    item[isYTd ? DATA_KEYS.VARIATION_YTD : DATA_KEYS.VARIATION_INTD];
  const dataValues = dataArray.map(callbackfn2);
  const ct = document.getElementById(ELEMENT_IDS.BARCHAR + index);
  const ctx = ct.getContext("2d");
  const colors = dataArray.map(callbackfn1);
  const chartData = getChartData(labels, dataValues, colors);
  const TIMEFRAME = isYTd ? "ANUAL" : "DIARIA";
  const MONEDA = dataArray[0][DATA_KEYS.MONEDA] === "ARS" ? "PESOS" : "DOLARES";
  const text = `Variación % por Fondo - ${tipo} - ${TIMEFRAME} - ${MONEDA}`;
  const options = getOptions(text);
  new Chart(ctx, {
    type: "bar",
    data: chartData,
    options: options,
  });
}

function callbackfn(i) {
  return {
    [DATA_KEYS.VARIATION_INTD]: i.ted,
    [DATA_KEYS.FUND_NAME]: i.nombre,
  };
}

function predicate(i) {
  return i.Fondo_Fondo.includes("IOL");
}

async function init() {
  const jsonScriptTags = document.getElementsByClassName(
    ELEMENT_IDS.FCI_DATA_CLASS,
  );
  const datosFinancierosElem = document.getElementById(
    ELEMENT_IDS.DATOS_FINANCIEROS,
  );
  const datosFinancierosData = JSON.parse(datosFinancierosElem.innerHTML);
  const x = BENCHMARK_GARANTIZADO_DATA.map(callbackfn);
  const callbackfn2 = (scriptTag, index) => {
    try {
      let rawFunds = JSON.parse(scriptTag.innerHTML);
      const tipo = extracted(rawFunds);
      const isYTD = scriptTag.id.includes("ytd");

      let variationKey;
      if (isYTD) {
        variationKey = DATA_KEYS.VARIATION_YTD;
        let inflacion;
        if (rawFunds[0]["Moneda Fondo_Moneda Fondo"] === "USD") {
          inflacion = {
            [DATA_KEYS.VARIATION_YTD]:
              datosFinancierosData["inflacion_usa_ytd_%"],
            [DATA_KEYS.FUND_NAME]: INFLACION + " USA 🔥",
          };
        } else {
          inflacion = {
            [DATA_KEYS.VARIATION_YTD]:
              datosFinancierosData.inflacion_uva["ytd_%"],
            [DATA_KEYS.FUND_NAME]: INFLACION + " UVA 🔥",
          };
        }
        rawFunds.push(inflacion);
      } else {
        variationKey = DATA_KEYS.VARIATION_INTD;
        let z = x;
        if (rawFunds[0]["Moneda Fondo_Moneda Fondo"] === "USD") {
          z = x.filter(predicate);
        }

        rawFunds = rawFunds.concat(z);
      }

      function compareFn(a, b) {
        const varA = a[variationKey] || 0;
        const varB = b[variationKey] || 0;
        return varB - varA;
      }

      const sortedFunds = [...rawFunds].sort(compareFn);
      const topFunds = sortedFunds.slice(0, TOP_FUNDS_LIMIT);
      crearDashboardHorizontalBarras(topFunds, index, isYTD, tipo);
    } catch (error) {
      console.error(
        `Error processing fund data from script tag ${index}:`,
        error,
      );
    }
  };
  Array.from(jsonScriptTags).forEach(callbackfn2);
}

document.addEventListener("DOMContentLoaded", init);
