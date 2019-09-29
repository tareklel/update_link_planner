from update_link_planner import UpdateLinkPlanner
from tarek_config import TarekConfig

if __name__ == '__main__':
    tarek_config = TarekConfig()

    UpdateLinkPlanner(
        titles=tarek_config.titles,
        key=tarek_config.key,
        period=tarek_config.period,
        limit=tarek_config.limit,
        save_destination=tarek_config.save_destination,
        uploaded_content=tarek_config.uploaded_content,
        link_building_planner=tarek_config.link_building_planner).run()
