import { Gauge, register} from 'prom-client'
import express from 'express'
import { S3Client, ListObjectsCommand } from "@aws-sdk/client-s3";
import { schedule } from 'node-cron'
import { config } from 'dotenv'

config()

type ConfigsType = ReturnType<typeof get_envs>
type MetricsType = ReturnType<typeof create_metrics>
type CommandsType = ReturnType<typeof create_commands>

const create_metrics = () => ({
    num_of_backups: new Gauge({
        name: 'number_of_dns_backups',
        help: 'total number of dns backups',
        labelNames: ["bucket", "dns_name"]
    }),
    total_size: new Gauge({
        name: 'dns_backup_size_total',
        help: 'total dns backup sizes',
        labelNames: ["bucket"]
    }),
    backup_size: new Gauge({
        name: 'dns_backup_size',
        help: 'dns backup size',
        labelNames: ["bucket", "dns_name"]
    }),
    backup_time: new Gauge({
        name: 'dns_backup_timestamp',
        help: 'dns backup timestamps',
        labelNames: ["bucket", "dns_name", "backup_size"]
    })
})

const create_commands = (configs: ConfigsType, client: S3Client) => {
    const get_total_metrics = async () => {
        const command = new ListObjectsCommand({Bucket: configs.S3_BUCKET})
        let res = await client.send(command)
        return {
            size: res.Contents ? res.Contents.reduce((prev, curr) => prev + (curr.Size ?? 0), 0) : 0
        }
    }

    const get_endpoint_metrics = async (prefix: string) => {
        const command = new ListObjectsCommand({Bucket: configs.S3_BUCKET, Prefix: prefix})
        let res = await client.send(command)
        return {
            prefix: prefix,
            size: res.Contents?.reduce((prev, curr) => prev + (curr.Size ?? 0), 0) ?? 0,
            length: res.Contents?.length ?? 0,
            timestamp: res.Contents?.reduce((prev, curr) => curr.LastModified ? Math.max(prev, curr.LastModified.getTime()) : prev, 0)
        }
    }

    const get_endpoint_metrics_array = async () => {
        const get_prefixes_list_command = new ListObjectsCommand({Bucket: process.env.S3_BUCKET, Delimiter: '/'})
        let prefixes = await client.send(get_prefixes_list_command)
        let prefix_metrics_list = prefixes.CommonPrefixes?.map(async data => get_endpoint_metrics(data.Prefix as string)) ?? []
        return Promise.all(prefix_metrics_list)
    }

    return { get_total_metrics, get_endpoint_metrics_array }
}

const create_s3_client = (configs: ConfigsType) => {
    return new S3Client({
        endpoint: configs.S3_ENDPOINT,
        credentials:{
            accessKeyId: configs.S3_ACCESS_KEY,
            secretAccessKey: configs.S3_SECRET_KEY
        },
        region: configs.S3_REGION
    })
}

const create_set_prom_metrics = (configs: ConfigsType, metrics: MetricsType, commands: CommandsType) => async () => {
    console.log('Job started!')
    const total_metrics = await commands.get_total_metrics()
    const endpoint_metrics_array = await commands.get_endpoint_metrics_array()

    metrics.total_size.set({bucket: configs.S3_BUCKET}, total_metrics.size)
    endpoint_metrics_array.forEach(endpoint_metrics => {
        metrics.backup_size.set({bucket: configs.S3_BUCKET, dns_name: endpoint_metrics.prefix}, endpoint_metrics.size)
        metrics.num_of_backups.set({bucket: configs.S3_BUCKET, dns_name: endpoint_metrics.prefix}, endpoint_metrics.length)
        if (endpoint_metrics.timestamp)
            metrics.backup_time.set({bucket: configs.S3_BUCKET, dns_name: endpoint_metrics.prefix}, endpoint_metrics.timestamp)
    })
    console.log('Job ended successfully.')
}

const get_envs = () => {
    const keys = ['S3_ACCESS_KEY', 'S3_SECRET_KEY', 'S3_ENDPOINT', 'S3_REGION', 'S3_BUCKET', 'CRON_EXPRESSION'] as const

    return keys.reduce((prev, curr) => {
        let val = process.env[curr]
        if (!val) throw Error(`[ERROR]: environment variable ${curr} is not provided`)
        return {...prev, [curr]: val}
    }, {} as {[k in (typeof keys)[number]]: string}) 
}

const run = async () => {
    const app = express()
    const configs = get_envs()
    const s3_client = create_s3_client(configs)
    const commands = create_commands(configs, s3_client)
    const metrics = create_metrics()
    const set_prom_metrics = create_set_prom_metrics(configs, metrics, commands)
    await set_prom_metrics()
    schedule(configs.CRON_EXPRESSION, set_prom_metrics)

    app.get('/metrics', async (req, res, next) => {
        res.setHeader('Content-Type',register.contentType)

        register.metrics().then(data => res.status(200).send(data))
    })

    app.listen(8000, () => console.log("listening on port 8000"))
}

run()