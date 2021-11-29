import pm2 from "pm2";

pm2.connect(function (err) {
  if (err) {
    console.error(err)
    process.exit(2)
  }
  for (let i = 0; i < 4; i++) {
    pm2.start({
      script: 'main.js',
      name: 'mainProcess',
      args: `${i * 23 + 1} ${(i + 1) * 23} `,
      autorestart: false,
    }, function (err, apps) {
      if (err) {
        console.error(err)
        return pm2.disconnect()
      }
    })
  }
})