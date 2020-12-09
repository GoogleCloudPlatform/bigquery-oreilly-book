const DATE = {
  date: `The date (UTC)`
};

const RAIN_MM = {
  rain_mm: `Daily rainfall, in mm`
};

const TMIN_CELSIUS = {
  tmin_celsius: `Daily minimum temperature, in Celsius`
};

const TMAX_CELSIUS = {
  tmax_celsius: `Daily maximum temperature, in Celsius`
};

// group documentation by table
const nyc_weather = {
  ...DATE,
  ...RAIN_MM,
  ...TMIN_CELSIUS,
  ...TMAX_CELSIUS,
};

module.exports = {
  nyc_weather
}
