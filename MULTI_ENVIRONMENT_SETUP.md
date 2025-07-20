# Multi-Environment Data Ingestion Setup

## 🔄 **Environment Strategy Overview**

The ELA Data Platform uses separate workflows and configurations for DEV and PRD environments to provide different execution frequencies and resource allocations optimized for each environment's needs.

## 🏗️ **Architecture**

### **Files Structure**
```
.github/workflows/
├── data-ingestion-dev.yaml     # DEV environment workflow
└── data-ingestion-prd.yaml     # PRD environment workflow

Configuration Files:
├── ingestion-config-dev.yaml   # DEV job configurations
├── ingestion-config-prd.yaml   # PRD job configurations
└── ingestion-config.yaml       # Legacy (can be removed)
```

### **Environment Differences**

| Aspect | DEV Environment | PRD Environment |
|--------|----------------|----------------|
| **Purpose** | Testing & Development | Production Data Collection |
| **Frequency** | More frequent (1-2 hours) | Optimized schedules (3-8 hours) |
| **Data Limits** | Smaller (20-100 items) | Full datasets (50-1000 items) |
| **Timeout** | 15 minutes | 45 minutes |
| **Parallelism** | 3 concurrent jobs | 5 concurrent jobs |
| **Retry Attempts** | 2 | 3 |
| **GCS Bucket** | `ela-dp-dev` | `ela-dp-prd` |
| **Error Retention** | 7 days | 14 days |

## 📊 **Job Frequency Comparison**

### **Spotify Jobs**
| Job | DEV Schedule | PRD Schedule | DEV Purpose | PRD Purpose |
|-----|-------------|-------------|------------|------------|
| Recently Played | Every hour | Every 3 hours | Frequent testing | Optimal data freshness |
| Saved Tracks | Daily 10:00 | Weekly Sunday 6:00 | Daily validation | Weekly full sync |
| Saved Albums | Daily 11:00 | Weekly Sunday 7:00 | Daily validation | Weekly full sync |
| Playlists | Weekly Mon 14:00 | Monthly 1st 8:00 | Weekly testing | Monthly comprehensive |
| Top Tracks | Weekly Mon 15:00 | Monthly 1st 9:00 | Weekly testing | Monthly analysis |
| Top Artists | Weekly Mon 16:00 | Monthly 1st 10:00 | Weekly testing | Monthly analysis |

### **Fitness Jobs**
| Job | DEV Schedule | PRD Schedule | DEV Purpose | PRD Purpose |
|-----|-------------|-------------|------------|------------|
| Strava Activities | Every 2 hours | Every 6 hours | Frequent testing | Regular sync |
| Strava Athlete | Daily 12:00 | Weekly Mon 6:00 | Daily validation | Weekly profile update |
| Garmin Activities | Every 2 hours | Every 4 hours | Frequent testing | Regular sync |
| Garmin Sleep | Daily 13:00 | Daily 8:00 | Daily testing | Daily sync |
| Garmin Heart Rate | Every 4 hours | Every 8 hours | Regular testing | Optimized sync |

### **Productivity Jobs**
| Job | DEV Schedule | PRD Schedule | DEV Purpose | PRD Purpose |
|-----|-------------|-------------|------------|------------|
| Todoist Tasks | Every 2 hours | Every 4 hours | Frequent testing | Regular sync |
| Todoist Projects | Daily 9:00 | Daily 7:00 | Daily testing | Daily sync |

## 🚀 **Usage Guide**

### **DEV Environment**
Use for:
- Testing new data sources
- Validating connector changes
- Development and debugging
- Frequent data validation

**Manual Trigger Options:**
- `frequent_jobs`: High-frequency jobs for testing
- `daily_jobs`: Daily validation jobs
- Individual job testing

### **PRD Environment** 
Use for:
- Production data collection
- Stable, reliable execution
- Cost-optimized schedules
- Business intelligence feeds

**Manual Trigger Options:**
- `weekly_jobs`: Weekly batch execution
- `monthly_jobs`: Monthly comprehensive sync
- Full production datasets

## 🔧 **Setup Instructions**

### **1. Enable Both Environments**
Both workflows run automatically every hour but check their respective configurations:
- DEV workflow reads `ingestion-config-dev.yaml`
- PRD workflow reads `ingestion-config-prd.yaml`

### **2. Manual Execution**

#### **Test in DEV First**
```bash
# Go to Actions → Data Ingestion DEV
# Select job group: "frequent_jobs"
# Run workflow
```

#### **Production Execution**
```bash
# Go to Actions → Data Ingestion PRD  
# Select job group: "weekly_jobs" or "monthly_jobs"
# Run workflow
```

### **3. Monitoring**
- **DEV**: Monitor for testing and validation
- **PRD**: Monitor for production data quality

## 📁 **GCS Upload Paths**

Each environment uploads to separate buckets and maintains service-specific paths:

### **DEV Environment**
- Spotify: `gs://ela-dp-dev/spotify/landing/`
- Strava: `gs://ela-dp-dev/strava/landing/`
- Garmin: `gs://ela-dp-dev/garmin/landing/`
- Todoist: `gs://ela-dp-dev/todoist/landing/`

### **PRD Environment**
- Spotify: `gs://ela-dp-prd/spotify/landing/`
- Strava: `gs://ela-dp-prd/strava/landing/`
- Garmin: `gs://ela-dp-prd/garmin/landing/`
- Todoist: `gs://ela-dp-prd/todoist/landing/`

## 🔄 **Migration from Single Environment**

If you previously used the single workflow:

### **1. Remove Legacy Files** (Optional)
```bash
# Remove the old single workflow
rm .github/workflows/data-ingestion-orchestrator.yaml
rm ingestion-config.yaml
```

### **2. Update Your Processes**
- Use DEV for testing: Manual triggers with frequent job groups
- Use PRD for production: Let it run automatically or trigger monthly jobs
- Monitor both environments separately

## 🎯 **Best Practices**

### **Development Workflow**
1. **Test in DEV**: Always test new configurations in DEV first
2. **Validate Data**: Check DEV output before deploying to PRD
3. **Monitor Frequently**: DEV jobs run often, monitor for issues
4. **Use Job Groups**: Leverage `frequent_jobs` for quick testing

### **Production Workflow**
1. **Stable Schedules**: PRD runs on optimized, stable schedules
2. **Monitor Failures**: PRD failures need immediate attention
3. **Resource Optimization**: PRD uses higher parallelism and longer timeouts
4. **Cost Management**: PRD schedules balance cost vs. data freshness

### **Cross-Environment**
1. **Same Secrets**: Both environments use the same GitHub secrets
2. **Separate Buckets**: Ensure bucket separation for data isolation
3. **Independent Execution**: Environments run independently
4. **Consistent Monitoring**: Monitor both environments consistently

## 🐛 **Troubleshooting**

### **Environment-Specific Issues**
- **DEV jobs failing**: Check if credentials work and reduce data limits
- **PRD jobs timing out**: Increase timeouts or reduce data volumes
- **Mixed uploads**: Verify correct bucket names in configurations

### **Configuration Validation**
Run validation scripts on both configs:
```bash
# Test DEV config
python test_ingestion_config.py ingestion-config-dev.yaml

# Test PRD config  
python test_ingestion_config.py ingestion-config-prd.yaml
```

## 📈 **Performance Optimization**

### **DEV Optimizations**
- Keep data limits small for faster execution
- Use frequent schedules for immediate feedback
- Enable all jobs for comprehensive testing

### **PRD Optimizations**
- Use optimal schedules based on data freshness needs
- Higher parallelism for efficiency
- Longer timeouts for large datasets
- Strategic retry attempts for reliability

## 🔒 **Security Considerations**

### **Shared Secrets**
Both environments use the same GitHub secrets but write to separate GCS buckets:
- Credentials are shared (same APIs)
- Data isolation through separate buckets
- Environment-specific monitoring and alerting

### **Access Control**
- DEV: More permissive for testing
- PRD: Strict monitoring and alerting
- Both: Same authentication mechanisms

## 🚀 **Future Enhancements**

### **Planned Improvements**
- **Branch-based triggers**: Auto-deploy DEV on feature branches
- **Environment promotion**: Promote validated configs from DEV to PRD
- **Cost monitoring**: Track execution costs per environment
- **Advanced scheduling**: Dynamic scheduling based on data patterns

### **Monitoring Integration**
- **Separate alerting**: Different alert thresholds for DEV vs PRD
- **Performance tracking**: Environment-specific performance metrics
- **Cost analysis**: Per-environment cost breakdown

This multi-environment setup provides a robust foundation for both development and production data ingestion workflows with appropriate resource allocation and scheduling optimization for each use case.